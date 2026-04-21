from __future__ import annotations

import argparse
import json
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
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

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
EMAIL_SYSTEM_PROMPT = """
Sei un assistente commerciale senior di Axend Group. Devi scrivere bozze email
post-call pronte da inviare a prospect B2B italiani, usando la trascrizione della
chiamata e, quando presente, il contesto dell'evento Calendar.

Obiettivo:
- produrre una email naturale, precisa e professionale;
- recuperare tutte le informazioni utili dalla call;
- usare gli esempi forniti solo come riferimento di struttura, tono e livello di
  dettaglio, non come fonte di fatti sul cliente corrente.

Regole di contenuto:
- Non inventare mai dati mancanti. Se un dato serve ma non e disponibile, inseriscilo
  in missing_fields e usa un placeholder chiaro nel corpo solo se indispensabile.
- Usa il registro emerso nella chiamata. Se non e chiaro, usa il lei.
- Mantieni prezzi, date, importi, prossimi step e richieste del cliente quando sono
  presenti nella trascrizione o nel Calendar.
- La partita IVA va inclusa se disponibile da Calendar o trascrizione. Se manca,
  segnala "Partita IVA" in missing_fields.
- Non promettere allegati se non risultano dalla call o dal Calendar. Se il cliente
  li ha chiesti, cita cosa andra allegato.
- Evita frasi generiche da brochure. La mail deve sembrare scritta dopo aver
  ascoltato davvero quella call.
- Non inserire citazioni, note, parentesi tipo [fonte] o riferimenti alle fonti
  dentro il corpo della email.
- Nel crm_payload, email, telefono, partita IVA e data follow-up completa devono
  essere valorizzati solo se compaiono esplicitamente nelle fonti. Se non compaiono,
  usa null e segnala il campo in missing_fields.
- Se la call menziona solo giorno e mese del follow-up, senza anno, lascia
  follow_up_date a null e riporta il testo nei next_steps.

Struttura consigliata del corpo email:
1. saluto e recap del motivo della mail;
2. "Analisi della situazione attuale" con contesto azienda, target, criticita,
   obiettivi e canali gia usati;
3. "Servizi proposti" con cosa consigliamo in base alla call;
4. "Come funzionerebbe l'attivita" con spiegazione operativa dei servizi;
5. "Investimento indicativo" se sono stati discussi prezzi o preventivo;
6. "Conclusione e prossimi passi" con follow-up, call successiva, allegati o azioni.

Output obbligatorio:
Rispondi solo con un JSON valido, senza markdown e senza testo esterno, con queste
chiavi:
{
  "subject": "oggetto email",
  "body": "corpo email pronto da inviare",
  "source_notes": ["note sintetiche sulle fonti usate, solo alla fine della bozza"],
  "missing_fields": ["campi utili mancanti"],
  "crm_payload": {
    "company_name": null,
    "contact_name": null,
    "vat_number": null,
    "email": null,
    "phone": null,
    "current_situation": null,
    "pain_points": [],
    "goals": [],
    "services_proposed": [],
    "prices_discussed": [],
    "next_steps": [],
    "follow_up_date": null,
    "decision_makers": [],
    "confidence": "low|medium|high"
  }
}
""".strip()


@dataclass(frozen=True)
class AppConfig:
    watch_dir: Path
    transcripts_dir: Path
    archive_dir: Optional[Path]
    email_drafts_enabled: bool
    email_drafts_dir: Path
    email_examples_dir: Optional[Path]
    calendar_context_dir: Optional[Path]
    delete_audio_after_processing: bool
    assemblyai_api_key: str
    openai_api_key: str
    openai_model: str
    email_model: str
    email_max_examples: int
    email_max_example_chars: int
    email_max_transcript_chars: int
    email_max_calendar_chars: int
    email_max_output_tokens: int
    poll_interval_seconds: float
    file_stability_interval_seconds: float
    file_stability_required_checks: int
    file_quiet_seconds: float
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
    openai_model = os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip() or "gpt-4o-mini"

    return AppConfig(
        watch_dir=env_path("WATCH_DIR", "./RECS"),
        transcripts_dir=env_path("TRANSCRIPTS_DIR", "./Trascrizioni"),
        archive_dir=env_optional_path("ARCHIVE_DIR", "./Archivio"),
        email_drafts_enabled=env_bool("EMAIL_DRAFTS_ENABLED", True),
        email_drafts_dir=env_path("EMAIL_DRAFTS_DIR", "./BozzeEmail"),
        email_examples_dir=env_optional_path("EMAIL_EXAMPLES_DIR", "./EsempiEmail"),
        calendar_context_dir=env_optional_path("CALENDAR_CONTEXT_DIR", "./EventiCalendar"),
        delete_audio_after_processing=env_bool("DELETE_AUDIO_AFTER_PROCESSING", False),
        assemblyai_api_key=assemblyai_api_key,
        openai_api_key=openai_api_key,
        openai_model=openai_model,
        email_model=os.getenv("EMAIL_MODEL", openai_model).strip() or openai_model,
        email_max_examples=env_int("EMAIL_MAX_EXAMPLES", 8),
        email_max_example_chars=env_int("EMAIL_MAX_EXAMPLE_CHARS", 8000),
        email_max_transcript_chars=env_int("EMAIL_MAX_TRANSCRIPT_CHARS", 120000),
        email_max_calendar_chars=env_int("EMAIL_MAX_CALENDAR_CHARS", 20000),
        email_max_output_tokens=env_int("EMAIL_MAX_OUTPUT_TOKENS", 6000),
        poll_interval_seconds=env_float("POLL_INTERVAL_SECONDS", 5.0),
        file_stability_interval_seconds=env_float("FILE_STABILITY_INTERVAL_SECONDS", 2.0),
        file_stability_required_checks=env_int("FILE_STABILITY_REQUIRED_CHECKS", 3),
        file_quiet_seconds=env_float("FILE_QUIET_SECONDS", 120.0),
        processing_timeout_seconds=env_int("PROCESSING_TIMEOUT_SECONDS", 10800),
        language_detection=env_bool("ASSEMBLYAI_LANGUAGE_DETECTION", True),
        speech_models=speech_models,
        speakers_expected=env_optional_int("ASSEMBLYAI_SPEAKERS_EXPECTED"),
    )


def ensure_directories(config: AppConfig) -> None:
    config.watch_dir.mkdir(parents=True, exist_ok=True)
    config.transcripts_dir.mkdir(parents=True, exist_ok=True)
    if config.email_drafts_enabled:
        config.email_drafts_dir.mkdir(parents=True, exist_ok=True)
        if config.email_examples_dir is not None:
            config.email_examples_dir.mkdir(parents=True, exist_ok=True)
        if config.calendar_context_dir is not None:
            config.calendar_context_dir.mkdir(parents=True, exist_ok=True)
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

        quiet_for = 0.0
        try:
            stat = path.stat()
            signature = (stat.st_size, stat.st_mtime_ns)
            quiet_for = time.time() - stat.st_mtime
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

        if (
            stable_checks >= config.file_stability_required_checks
            and quiet_for >= config.file_quiet_seconds
        ):
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
        payload["speech_models"] = list(config.speech_models)
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


def truncate_text(text: str, max_chars: int) -> str:
    clean_text = text.strip()
    if max_chars <= 0 or len(clean_text) <= max_chars:
        return clean_text

    half = max_chars // 2
    remaining = max_chars - half
    return (
        clean_text[:half].rstrip()
        + "\n\n[... contenuto tagliato per limiti di contesto ...]\n\n"
        + clean_text[-remaining:].lstrip()
    )


def read_text(path: Path, max_chars: int) -> str:
    return truncate_text(path.read_text(encoding="utf-8"), max_chars)


def load_email_examples(config: AppConfig) -> List[Tuple[Path, str]]:
    if config.email_examples_dir is None or not config.email_examples_dir.exists():
        return []

    supported_suffixes = {".txt", ".md", ".eml"}
    examples: List[Tuple[Path, str]] = []
    for path in sorted(config.email_examples_dir.iterdir()):
        if len(examples) >= config.email_max_examples:
            break
        if not path.is_file() or path.suffix.lower() not in supported_suffixes:
            continue
        try:
            examples.append((path, read_text(path, config.email_max_example_chars)))
        except UnicodeDecodeError:
            logging.warning("Skipping email example with unsupported encoding: %s", path)

    return examples


def find_calendar_context(
    config: AppConfig,
    lookup_stems: Sequence[str],
) -> Tuple[Optional[Path], str]:
    if config.calendar_context_dir is None or not config.calendar_context_dir.exists():
        return None, ""

    candidates: List[str] = []
    for stem in lookup_stems:
        clean_stem = stem.strip()
        if not clean_stem:
            continue
        candidates.append(clean_stem)
        candidates.append(sanitize_filename(clean_stem))

    seen: Set[str] = set()
    for stem in candidates:
        if stem in seen:
            continue
        seen.add(stem)
        for suffix in (".txt", ".md", ".json"):
            path = config.calendar_context_dir / f"{stem}{suffix}"
            if path.exists() and path.is_file():
                return path, read_text(path, config.email_max_calendar_chars)

    return None, ""


def format_email_examples(examples: Sequence[Tuple[Path, str]]) -> str:
    if not examples:
        return "Nessun esempio email fornito."

    chunks = []
    for index, (path, content) in enumerate(examples, start=1):
        chunks.append(
            "\n".join(
                [
                    f"ESEMPIO {index}: {path.name}",
                    "---",
                    content,
                    "---",
                ]
            )
        )
    return "\n\n".join(chunks)


def build_email_user_prompt(
    transcript_text: str,
    transcript_path: Path,
    calendar_path: Optional[Path],
    calendar_context: str,
    examples: Sequence[Tuple[Path, str]],
    config: AppConfig,
) -> str:
    calendar_label = calendar_path.name if calendar_path is not None else "non disponibile"
    calendar_text = calendar_context or "Nessun contesto Calendar trovato per questa trascrizione."

    return "\n\n".join(
        [
            "Devi generare la bozza email per questa call.",
            f"FILE TRASCRIZIONE: {transcript_path.name}",
            f"CONTESTO CALENDAR: {calendar_label}",
            "=== CONTESTO CALENDAR ===",
            truncate_text(calendar_text, config.email_max_calendar_chars),
            "=== TRASCRIZIONE CALL ===",
            truncate_text(transcript_text, config.email_max_transcript_chars),
            "=== ESEMPI EMAIL DA IMITARE PER STILE E STRUTTURA ===",
            format_email_examples(examples),
        ]
    )


def parse_json_object(raw_text: str) -> Dict[str, Any]:
    clean_text = raw_text.strip()
    if clean_text.startswith("```"):
        clean_text = re.sub(r"^```(?:json)?\s*", "", clean_text, flags=re.IGNORECASE)
        clean_text = re.sub(r"\s*```$", "", clean_text)

    try:
        parsed = json.loads(clean_text)
    except json.JSONDecodeError:
        first_brace = clean_text.find("{")
        last_brace = clean_text.rfind("}")
        if first_brace == -1 or last_brace == -1 or last_brace <= first_brace:
            raise
        parsed = json.loads(clean_text[first_brace : last_brace + 1])

    if not isinstance(parsed, dict):
        raise ValueError("OpenAI response JSON is not an object")
    return parsed


def coerce_string_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    clean_value = str(value).strip()
    return [clean_value] if clean_value else []


def add_missing_field(draft: Dict[str, Any], field_name: str) -> None:
    missing_fields = coerce_string_list(draft.get("missing_fields"))
    existing = {field.lower() for field in missing_fields}
    if field_name.lower() not in existing:
        missing_fields.append(field_name)
    draft["missing_fields"] = missing_fields


def normalize_digits(value: Any) -> str:
    return re.sub(r"\D+", "", str(value or ""))


def extract_source_emails(source_text: str) -> Set[str]:
    return {
        match.lower()
        for match in re.findall(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", source_text, re.IGNORECASE)
    }


def extract_source_vat_numbers(source_text: str) -> Set[str]:
    vat_numbers = set()
    for match in re.findall(r"\b(?:\d[\s.-]?){11}\b", source_text):
        digits = normalize_digits(match)
        if len(digits) == 11:
            vat_numbers.add(digits)
    return vat_numbers


def source_contains_digit_value(value: Any, source_text: str, min_digits: int = 6) -> bool:
    digits = normalize_digits(value)
    return len(digits) >= min_digits and digits in normalize_digits(source_text)


def source_contains_complete_year(value: Any, source_text: str) -> bool:
    text_value = str(value or "")
    years = re.findall(r"\b(?:19|20)\d{2}\b", text_value)
    return bool(years) and all(year in source_text for year in years)


def enforce_source_backed_crm_fields(draft: Dict[str, Any], source_text: str) -> None:
    crm_payload = draft.get("crm_payload")
    if not isinstance(crm_payload, dict):
        crm_payload = {}
        draft["crm_payload"] = crm_payload

    source_emails = extract_source_emails(source_text)
    email = str(crm_payload.get("email") or "").strip().lower()
    if email and email not in source_emails:
        crm_payload["email"] = None
        add_missing_field(draft, "Email cliente")
    elif not email and len(source_emails) == 1:
        crm_payload["email"] = next(iter(source_emails))
    elif not email:
        add_missing_field(draft, "Email cliente")

    source_vats = extract_source_vat_numbers(source_text)
    vat_number = normalize_digits(crm_payload.get("vat_number"))
    if vat_number and vat_number not in source_vats:
        crm_payload["vat_number"] = None
        add_missing_field(draft, "Partita IVA")
    elif not vat_number and len(source_vats) == 1:
        crm_payload["vat_number"] = next(iter(source_vats))
    elif not vat_number:
        add_missing_field(draft, "Partita IVA")

    phone = crm_payload.get("phone")
    if phone and not source_contains_digit_value(phone, source_text):
        crm_payload["phone"] = None

    follow_up_date = crm_payload.get("follow_up_date")
    if follow_up_date and not source_contains_complete_year(follow_up_date, source_text):
        crm_payload["follow_up_date"] = None
        add_missing_field(draft, "Data follow-up completa")


def normalize_email_draft(raw_text: str) -> Dict[str, Any]:
    try:
        draft = parse_json_object(raw_text)
    except Exception:
        logging.exception("OpenAI did not return parseable email JSON")
        return {
            "subject": "Follow-up alla nostra call",
            "body": raw_text.strip(),
            "source_notes": ["Risposta OpenAI non in formato JSON: verificare manualmente la bozza."],
            "missing_fields": ["Verifica manuale formato bozza"],
            "crm_payload": {"confidence": "low"},
        }

    crm_payload = draft.get("crm_payload")
    if not isinstance(crm_payload, dict):
        crm_payload = {}

    return {
        "subject": str(draft.get("subject") or "Follow-up alla nostra call").strip(),
        "body": str(draft.get("body") or "").strip(),
        "source_notes": coerce_string_list(draft.get("source_notes")),
        "missing_fields": coerce_string_list(draft.get("missing_fields")),
        "crm_payload": crm_payload,
    }


def generate_sales_email_draft(
    transcript_text: str,
    transcript_path: Path,
    calendar_path: Optional[Path],
    calendar_context: str,
    examples: Sequence[Tuple[Path, str]],
    config: AppConfig,
) -> Dict[str, Any]:
    logging.info("Generating sales email draft with OpenAI model %s", config.email_model)
    client = OpenAI(api_key=config.openai_api_key)
    response = client.responses.create(
        model=config.email_model,
        input=[
            {
                "role": "system",
                "content": [{"type": "input_text", "text": EMAIL_SYSTEM_PROMPT}],
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": build_email_user_prompt(
                            transcript_text=transcript_text,
                            transcript_path=transcript_path,
                            calendar_path=calendar_path,
                            calendar_context=calendar_context,
                            examples=examples,
                            config=config,
                        ),
                    }
                ],
            },
        ],
        temperature=0.2,
        max_output_tokens=config.email_max_output_tokens,
    )

    draft_text = extract_openai_text(response)
    if not draft_text:
        raise RuntimeError("OpenAI returned an empty email draft")

    draft = normalize_email_draft(draft_text)
    enforce_source_backed_crm_fields(draft, "\n\n".join([transcript_text, calendar_context]))
    draft["source_transcript_file"] = transcript_path.name
    draft["source_calendar_file"] = calendar_path.name if calendar_path is not None else None
    draft["examples_used"] = [path.name for path, _ in examples]
    return draft


def format_email_draft_markdown(draft: Dict[str, Any]) -> str:
    source_notes = coerce_string_list(draft.get("source_notes"))
    missing_fields = coerce_string_list(draft.get("missing_fields"))

    lines = [
        "# Bozza email",
        "",
        f"**Oggetto:** {draft.get('subject', 'Follow-up alla nostra call')}",
        "",
        str(draft.get("body") or "").strip(),
        "",
        "---",
        "",
        "## Fonti e controlli",
    ]

    if source_notes:
        lines.extend(f"- {note}" for note in source_notes)
    else:
        lines.append("- Nessuna nota fonte restituita dal modello.")

    lines.extend(
        [
            "",
            f"- Trascrizione: {draft.get('source_transcript_file')}",
            f"- Calendar: {draft.get('source_calendar_file') or 'non disponibile'}",
            f"- Esempi email: {', '.join(draft.get('examples_used') or []) or 'non disponibili'}",
            "",
            "## Campi mancanti",
        ]
    )

    if missing_fields:
        lines.extend(f"- {field}" for field in missing_fields)
    else:
        lines.append("- Nessuno")

    return "\n".join(lines).strip() + "\n"


def save_email_outputs(
    draft: Dict[str, Any],
    title: str,
    config: AppConfig,
) -> Tuple[Path, Path]:
    config.email_drafts_dir.mkdir(parents=True, exist_ok=True)
    safe_title = sanitize_filename(title)
    draft_path = unique_path(config.email_drafts_dir, f"{safe_title}_email", ".md")
    json_path = draft_path.with_suffix(".json")

    draft_path.write_text(format_email_draft_markdown(draft), encoding="utf-8")
    json_path.write_text(json.dumps(draft, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    logging.info("Saved email draft: %s", draft_path)
    logging.info("Saved CRM payload draft: %s", json_path)
    return draft_path, json_path


def generate_and_save_email_draft(
    transcript_path: Path,
    transcript_text: str,
    title: str,
    config: AppConfig,
    extra_calendar_lookup_stems: Sequence[str] = (),
    force: bool = False,
) -> Optional[Tuple[Path, Path]]:
    if not config.email_drafts_enabled and not force:
        return None

    lookup_stems = [transcript_path.stem, title, *extra_calendar_lookup_stems]
    calendar_path, calendar_context = find_calendar_context(config, lookup_stems)
    examples = load_email_examples(config)
    draft = generate_sales_email_draft(
        transcript_text=transcript_text,
        transcript_path=transcript_path,
        calendar_path=calendar_path,
        calendar_context=calendar_context,
        examples=examples,
        config=config,
    )
    return save_email_outputs(draft, title, config)


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
    transcript_path = save_transcript(transcript_text, title, config)
    try:
        generate_and_save_email_draft(
            transcript_path=transcript_path,
            transcript_text=transcript_text,
            title=title,
            config=config,
            extra_calendar_lookup_stems=(audio_path.stem,),
        )
    except Exception:
        logging.exception("Failed to generate email draft for %s", transcript_path)
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
    parser.add_argument(
        "--draft-email-from-transcript",
        type=Path,
        help="Generate an email draft from an existing transcript .txt and exit.",
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
        if args.once and args.draft_email_from_transcript:
            raise ConfigError("Use either --once or --draft-email-from-transcript, not both")
        if args.draft_email_from_transcript:
            transcript_path = args.draft_email_from_transcript.expanduser().resolve()
            transcript_text = transcript_path.read_text(encoding="utf-8")
            generate_and_save_email_draft(
                transcript_path=transcript_path,
                transcript_text=transcript_text,
                title=transcript_path.stem,
                config=config,
                force=True,
            )
        elif args.once:
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
