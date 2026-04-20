from __future__ import annotations

import argparse
import logging
import os
import re
import shutil
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from queue import Queue
from typing import Any, Dict, Iterable, Optional, Set, Tuple

import requests
from dotenv import load_dotenv
from openai import OpenAI
from watchdog.events import FileSystemEvent, FileSystemEventHandler, FileMovedEvent
from watchdog.observers import Observer


ASSEMBLYAI_BASE_URL = "https://api.assemblyai.com"
TITLE_SYSTEM_PROMPT = (
    "Sei un assistente. Leggi questa trascrizione di una call di vendita. "
    "Trova il nome del potenziale cliente (se menzionato) e il prodotto/servizio "
    "principale di cui si parla. Rispondi SOLO con un nome file nel formato: "
    "Nome_Cognome_Prodotto. Nessun'altra parola."
)


@dataclass(frozen=True)
class AppConfig:
    watch_dir: Path
    transcripts_dir: Path
    archive_dir: Optional[Path]
    delete_audio_after_processing: bool
    assemblyai_api_key: str
    openai_api_key: str
    openai_model: str
    poll_interval_seconds: float
    file_stability_interval_seconds: float
    file_stability_required_checks: int
    processing_timeout_seconds: int
    language_detection: bool
    speech_models: Tuple[str, ...]
    speakers_expected: Optional[int]


class ConfigError(RuntimeError):
    pass


class AssemblyAIError(RuntimeError):
    pass


def env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    try:
        return float(value)
    except ValueError as exc:
        raise ConfigError(f"{name} must be a number") from exc


def env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    try:
        return int(value)
    except ValueError as exc:
        raise ConfigError(f"{name} must be an integer") from exc


def env_optional_int(name: str) -> Optional[int]:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return None
    try:
        return int(value)
    except ValueError as exc:
        raise ConfigError(f"{name} must be an integer") from exc


def env_path(name: str, default: str) -> Path:
    return Path(os.getenv(name, default)).expanduser().resolve()


def env_optional_path(name: str, default: str) -> Optional[Path]:
    value = os.getenv(name, default)
    if value.strip() == "":
        return None
    return Path(value).expanduser().resolve()


def load_config() -> AppConfig:
    load_dotenv()

    assemblyai_api_key = os.getenv("ASSEMBLYAI_API_KEY", "").strip()
    openai_api_key = os.getenv("OPENAI_API_KEY", "").strip()

    missing = []
    if not assemblyai_api_key:
        missing.append("ASSEMBLYAI_API_KEY")
    if not openai_api_key:
        missing.append("OPENAI_API_KEY")
    if missing:
        joined = ", ".join(missing)
        raise ConfigError(f"Missing required environment variable(s): {joined}")

    speech_models = tuple(
        model.strip()
        for model in os.getenv("ASSEMBLYAI_SPEECH_MODELS", "universal-3-pro,universal-2").split(",")
        if model.strip()
    )

    return AppConfig(
        watch_dir=env_path("WATCH_DIR", "./RECS"),
        transcripts_dir=env_path("TRANSCRIPTS_DIR", "./Trascrizioni"),
        archive_dir=env_optional_path("ARCHIVE_DIR", "./Archivio"),
        delete_audio_after_processing=env_bool("DELETE_AUDIO_AFTER_PROCESSING", False),
        assemblyai_api_key=assemblyai_api_key,
        openai_api_key=openai_api_key,
        openai_model=os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip() or "gpt-4o-mini",
        poll_interval_seconds=env_float("POLL_INTERVAL_SECONDS", 5.0),
        file_stability_interval_seconds=env_float("FILE_STABILITY_INTERVAL_SECONDS", 2.0),
        file_stability_required_checks=env_int("FILE_STABILITY_REQUIRED_CHECKS", 3),
        processing_timeout_seconds=env_int("PROCESSING_TIMEOUT_SECONDS", 10800),
        language_detection=env_bool("ASSEMBLYAI_LANGUAGE_DETECTION", True),
        speech_models=speech_models,
        speakers_expected=env_optional_int("ASSEMBLYAI_SPEAKERS_EXPECTED"),
    )


def ensure_directories(config: AppConfig) -> None:
    config.watch_dir.mkdir(parents=True, exist_ok=True)
    config.transcripts_dir.mkdir(parents=True, exist_ok=True)
    if config.archive_dir is not None and not config.delete_audio_after_processing:
        config.archive_dir.mkdir(parents=True, exist_ok=True)


def wait_for_file_complete(path: Path, config: AppConfig) -> None:
    logging.info("Waiting for OBS to finish writing %s", path.name)
    started_at = time.monotonic()
    last_signature: Optional[Tuple[int, int]] = None
    stable_checks = 0

    while True:
        if time.monotonic() - started_at > config.processing_timeout_seconds:
            raise TimeoutError(f"Timed out waiting for file to become stable: {path}")

        try:
            stat = path.stat()
            signature = (stat.st_size, stat.st_mtime_ns)
            if stat.st_size <= 0:
                stable_checks = 0
            elif signature == last_signature:
                stable_checks += 1
            else:
                stable_checks = 0
                last_signature = signature

            with path.open("rb") as handle:
                handle.seek(0, os.SEEK_END)
        except FileNotFoundError:
            stable_checks = 0
            last_signature = None
        except OSError:
            stable_checks = 0

        if stable_checks >= config.file_stability_required_checks:
            logging.info("File is stable: %s", path.name)
            return

        time.sleep(config.file_stability_interval_seconds)


def assemblyai_headers(config: AppConfig) -> Dict[str, str]:
    return {"authorization": config.assemblyai_api_key}


def upload_audio_to_assemblyai(audio_path: Path, config: AppConfig) -> str:
    logging.info("Uploading audio to AssemblyAI: %s", audio_path.name)
    upload_url = f"{ASSEMBLYAI_BASE_URL}/v2/upload"

    with audio_path.open("rb") as audio_file:
        response = requests.post(
            upload_url,
            headers=assemblyai_headers(config),
            data=audio_file,
            timeout=(10, 600),
        )

    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        raise AssemblyAIError(f"AssemblyAI upload failed: {response.text}") from exc

    upload_result = response.json()
    audio_url = upload_result.get("upload_url")
    if not audio_url:
        raise AssemblyAIError(f"AssemblyAI upload response missing upload_url: {upload_result}")
    return str(audio_url)


def request_assemblyai_transcript(audio_url: str, config: AppConfig) -> str:
    payload: Dict[str, Any] = {
        "audio_url": audio_url,
        "speaker_labels": True,
        "language_detection": config.language_detection,
    }
    if config.speech_models:
        payload["speech_model"] = config.speech_models[0]
    if config.speakers_expected is not None:
        payload["speakers_expected"] = config.speakers_expected

    response = requests.post(
        f"{ASSEMBLYAI_BASE_URL}/v2/transcript",
        headers={**assemblyai_headers(config), "content-type": "application/json"},
        json=payload,
        timeout=30,
    )

    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        raise AssemblyAIError(f"AssemblyAI transcript request failed: {response.text}") from exc

    transcript_result = response.json()
    transcript_id = transcript_result.get("id")
    if not transcript_id:
        raise AssemblyAIError(f"AssemblyAI transcript response missing id: {transcript_result}")
    return str(transcript_id)


def poll_assemblyai_transcript(transcript_id: str, config: AppConfig) -> Dict[str, Any]:
    logging.info("Waiting for AssemblyAI transcript %s", transcript_id)
    polling_url = f"{ASSEMBLYAI_BASE_URL}/v2/transcript/{transcript_id}"
    started_at = time.monotonic()
    last_status = None

    while True:
        if time.monotonic() - started_at > config.processing_timeout_seconds:
            raise TimeoutError(f"Timed out waiting for transcript: {transcript_id}")

        response = requests.get(
            polling_url,
            headers=assemblyai_headers(config),
            timeout=30,
        )

        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            raise AssemblyAIError(f"AssemblyAI polling failed: {response.text}") from exc

        transcript = response.json()
        status = transcript.get("status")
        if status != last_status:
            logging.info("AssemblyAI status for %s: %s", transcript_id, status)
            last_status = status

        if status == "completed":
            return transcript
        if status == "error":
            error_message = transcript.get("error", "Unknown AssemblyAI error")
            raise AssemblyAIError(f"AssemblyAI transcription failed: {error_message}")

        time.sleep(config.poll_interval_seconds)


def transcribe_audio(audio_path: Path, config: AppConfig) -> str:
    audio_url = upload_audio_to_assemblyai(audio_path, config)
    transcript_id = request_assemblyai_transcript(audio_url, config)
    transcript = poll_assemblyai_transcript(transcript_id, config)
    return format_transcript(transcript)


def format_transcript(transcript: Dict[str, Any]) -> str:
    utterances = transcript.get("utterances") or []
    if utterances:
        lines = []
        for utterance in utterances:
            speaker = str(utterance.get("speaker", "?")).strip() or "?"
            text = str(utterance.get("text", "")).strip()
            if text:
                lines.append(f"Speaker {speaker}: {text}")
        if lines:
            return "\n\n".join(lines)

    text = str(transcript.get("text", "")).strip()
    if text:
        return text

    raise AssemblyAIError("AssemblyAI returned an empty transcript")


def extract_openai_text(response: Any) -> str:
    output_text = getattr(response, "output_text", None)
    if output_text:
        return str(output_text).strip()

    if hasattr(response, "model_dump"):
        data = response.model_dump()
    else:
        data = response

    chunks = []
    for item in data.get("output", []):
        for content in item.get("content", []):
            text = content.get("text")
            if text:
                chunks.append(text)
    return "\n".join(chunks).strip()


def generate_file_title(transcript_text: str, config: AppConfig) -> str:
    logging.info("Generating file name with OpenAI model %s", config.openai_model)
    client = OpenAI(api_key=config.openai_api_key)
    response = client.responses.create(
        model=config.openai_model,
        input=[
            {
                "role": "system",
                "content": [{"type": "input_text", "text": TITLE_SYSTEM_PROMPT}],
            },
            {
                "role": "user",
                "content": [{"type": "input_text", "text": transcript_text}],
            },
        ],
        temperature=0,
        max_output_tokens=64,
    )

    title = extract_openai_text(response)
    if not title:
        raise RuntimeError("OpenAI returned an empty file name")
    return title


def sanitize_filename(raw_name: str) -> str:
    name = raw_name.strip().strip("'\"")
    if name.lower().endswith(".txt"):
        name = name[:-4]

    name = name.replace(" ", "_")
    cleaned_chars = []
    for char in name:
        if char in '<>:"/\\|?*' or ord(char) < 32:
            cleaned_chars.append("_")
        else:
            cleaned_chars.append(char)

    cleaned = "".join(cleaned_chars)
    cleaned = re.sub(r"_+", "_", cleaned).strip("._ ")
    return cleaned[:140] or "Trascrizione"


def unique_path(directory: Path, stem: str, suffix: str) -> Path:
    candidate = directory / f"{stem}{suffix}"
    counter = 2
    while candidate.exists():
        candidate = directory / f"{stem}_{counter}{suffix}"
        counter += 1
    return candidate


def save_transcript(transcript_text: str, title: str, config: AppConfig) -> Path:
    safe_title = sanitize_filename(title)
    if safe_title != title.strip():
        logging.warning("Sanitized OpenAI file name from %r to %r", title, safe_title)

    output_path = unique_path(config.transcripts_dir, safe_title, ".txt")
    output_path.write_text(transcript_text + "\n", encoding="utf-8")
    logging.info("Saved transcript: %s", output_path)
    return output_path


def archive_or_delete_audio(audio_path: Path, config: AppConfig) -> None:
    if config.delete_audio_after_processing:
        audio_path.unlink()
        logging.info("Deleted original audio: %s", audio_path.name)
        return

    if config.archive_dir is None:
        logging.info("Leaving original audio in place: %s", audio_path)
        return

    archive_path = unique_path(config.archive_dir, audio_path.stem, audio_path.suffix)
    shutil.move(str(audio_path), str(archive_path))
    logging.info("Moved original audio to archive: %s", archive_path)


def process_audio_file(audio_path: Path, config: AppConfig) -> None:
    if audio_path.suffix.lower() != ".mp3":
        return

    logging.info("Processing audio file: %s", audio_path)
    wait_for_file_complete(audio_path, config)
    transcript_text = transcribe_audio(audio_path, config)
    title = generate_file_title(transcript_text, config)
    save_transcript(transcript_text, title, config)
    archive_or_delete_audio(audio_path, config)
    logging.info("Finished processing: %s", audio_path.name)


class MP3WatchHandler(FileSystemEventHandler):
    def __init__(self, work_queue: Queue[Path], queued_paths: Set[Path], lock: threading.Lock) -> None:
        self.work_queue = work_queue
        self.queued_paths = queued_paths
        self.lock = lock

    def on_created(self, event: FileSystemEvent) -> None:
        self.queue_event_path(event)

    def on_moved(self, event: FileMovedEvent) -> None:
        self.queue_path(Path(event.dest_path))

    def queue_event_path(self, event: FileSystemEvent) -> None:
        if event.is_directory:
            return
        self.queue_path(Path(event.src_path))

    def queue_path(self, path: Path) -> None:
        resolved = path.expanduser().resolve()
        if resolved.suffix.lower() != ".mp3":
            return

        with self.lock:
            if resolved in self.queued_paths:
                return
            self.queued_paths.add(resolved)

        logging.info("Queued new MP3: %s", resolved)
        self.work_queue.put(resolved)


def worker_loop(work_queue: Queue[Optional[Path]], queued_paths: Set[Path], lock: threading.Lock, config: AppConfig) -> None:
    while True:
        audio_path = work_queue.get()
        try:
            if audio_path is None:
                return
            try:
                process_audio_file(audio_path, config)
            except Exception:
                logging.exception("Failed to process %s", audio_path)
            finally:
                with lock:
                    queued_paths.discard(audio_path)
        finally:
            work_queue.task_done()


def enqueue_existing_mp3_files(
    watch_dir: Path,
    work_queue: Queue[Path],
    queued_paths: Set[Path],
    lock: threading.Lock,
) -> None:
    for path in sorted(watch_dir.glob("*.mp3")):
        resolved = path.resolve()
        with lock:
            if resolved in queued_paths:
                continue
            queued_paths.add(resolved)
        logging.info("Queued existing MP3: %s", resolved)
        work_queue.put(resolved)


def run_watcher(config: AppConfig) -> None:
    ensure_directories(config)

    work_queue: Queue[Optional[Path]] = Queue()
    queued_paths: Set[Path] = set()
    lock = threading.Lock()
    worker = threading.Thread(
        target=worker_loop,
        args=(work_queue, queued_paths, lock, config),
        daemon=True,
    )
    worker.start()

    enqueue_existing_mp3_files(config.watch_dir, work_queue, queued_paths, lock)

    event_handler = MP3WatchHandler(work_queue, queued_paths, lock)
    observer = Observer()
    observer.schedule(event_handler, str(config.watch_dir), recursive=False)
    observer.start()

    logging.info("Watching folder: %s", config.watch_dir)
    logging.info("Transcripts folder: %s", config.transcripts_dir)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Stopping watcher")
    finally:
        observer.stop()
        observer.join()
        work_queue.put(None)
        work_queue.join()


def parse_args(argv: Iterable[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Transcribe and rename OBS MP3 recordings.")
    parser.add_argument(
        "--once",
        type=Path,
        help="Process one MP3 file and exit instead of watching WATCH_DIR.",
    )
    return parser.parse_args(list(argv))


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def main(argv: Iterable[str] = sys.argv[1:]) -> int:
    configure_logging()
    args = parse_args(argv)

    try:
        config = load_config()
        ensure_directories(config)
        if args.once:
            process_audio_file(args.once.expanduser().resolve(), config)
        else:
            run_watcher(config)
        return 0
    except ConfigError as exc:
        logging.error("%s", exc)
        logging.error("Create a .env file from .env.example and add your API keys.")
        return 2
    except KeyboardInterrupt:
        return 130
    except Exception:
        logging.exception("Fatal error")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
