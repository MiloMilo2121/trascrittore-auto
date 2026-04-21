from __future__ import annotations

import argparse
import html
import json
import logging
import os
import re
import shutil
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from queue import Queue
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple
from urllib.parse import quote_plus, urlparse

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
FACT_EXTRACTION_SYSTEM_PROMPT = """
Sei un analista commerciale di Axend Group. Ricevi dati Calendar, trascrizione,
dati lead eventualmente gia noti e ricerca web. Devi estrarre e normalizzare i
dati prima che venga scritta la mail.

Ordine di priorita delle fonti:
1. Google Calendar
2. Trascrizione call
3. Dati gia forniti
4. Sito ufficiale azienda
5. Fonte esterna per P.IVA

Regole:
- Non inventare mai dati mancanti.
- Se un dato manca, usa "Non reperito".
- Se un dato e incerto o in conflitto, usa "Da verificare".
- Email, telefono, P.IVA e date complete devono essere valorizzati solo se
  compaiono nelle fonti o nella ricerca web verificata.
- Se la call cita solo giorno e mese di un follow-up senza anno, metti
  next_meeting_text con la formula testuale e next_meeting_date a "Da verificare".
- Non aggiungere servizi non emersi. I servizi tipici sono "Analisi Basic",
  "Analisi Sales Map", "Telesetting Advanced", ma usali solo se coerenti con
  call e Calendar.

Rispondi solo con JSON valido, senza markdown e senza testo esterno, con queste
chiavi:
{
  "company_name": "Non reperito",
  "seller_name": "Non reperito",
  "contact_name": "Non reperito",
  "contact_first_name": "Non reperito",
  "phone": "Non reperito",
  "email": "Non reperito",
  "vat_number": "Non reperito",
  "vat_source": "Non reperito",
  "proposed_services": [],
  "close_confidence": "Non reperito",
  "close_date": "Non reperito",
  "next_meeting_date": "Non reperito",
  "next_meeting_text": "Non reperito",
  "needs": [],
  "current_issues": [],
  "goals": [],
  "current_situation": "Non reperito",
  "decision_makers": [],
  "attachments_to_send": [],
  "missing_or_to_verify": []
}
""".strip()

EMAIL_SYSTEM_PROMPT = """
Sei un'AI che scrive email di follow-up post discovery call.

Scrivi SEMPRE come se fossi Francesca, collega del commerciale che ha svolto la
call. Il tono deve essere cordiale ma estremamente professionale. Dare sempre
del "Lei". Nessuna battuta. Nessun link nel corpo del testo.

Ricevi:
1. dati evento Google Calendar
2. trascrizione della call
3. eventuali dati gia noti del lead
4. eventuale ricerca web o fonte esterna per verifica P.IVA
5. dati estratti e normalizzati nella fase precedente

Ordine di priorita delle fonti:
1. Google Calendar
2. Trascrizione call
3. Dati gia forniti
4. Sito ufficiale azienda
5. Fonte esterna per P.IVA

Regole fondamentali:
- Non inventare mai dati mancanti.
- Se un dato manca, scrivi "Non reperito".
- Se un dato e incerto o in conflitto, scrivi "Da verificare".
- Non aggiungere servizi non emersi.
- Non inserire link nel corpo della mail.
- Se serve citare la fonte della P.IVA, fallo solo in fondo, in una riga separata.
- Usa sempre il "Lei" e le forme "Vostro/Vostra/Vostri" quando ti riferisci
  all'azienda del prospect.
- Fai sempre riferimento alla call con il commerciale corretto.
- Scrivi la mail come testo pronto per essere inviato da Francesca.

La mail deve avere questa logica:
1. Saluto e ringraziamento per la call, con richiamo concreto alla loro realta.
2. Analisi della Situazione Attuale: riassumi il contesto e spiega perche il
   modello attuale e fragile, inefficiente, poco prevedibile o non scalabile.
3. Obiettivi: chiarisci cosa vogliono ottenere.
4. Percorso Operativo:
   - Analisi: spiega output e utilita nel loro caso specifico.
   - Telesetting: spiega come funziona e perche aiuta a raggiungere l'obiettivo.
5. Conclusione: indica che in allegato c'e la proposta economica solo come
   riferimento scritto, per evitare dubbi e facilitare il confronto interno.
6. Se esiste un incontro gia fissato, confermalo. Specifica che sara presente
   uno dei soci, perche siamo una realta esclusiva che collabora con un solo
   partner per settore e teniamo a conoscere personalmente la proprieta prima
   di partire.

Usa questo formato di output ESATTO:

{{nome azienda}}

Venditore: {{venditore}}

Riferimento: {{persona con cui si e parlato}}

Tel: {{telefono}}

Mail: {{email referente}}

Partita IVA: {{piva}}

Servizi proposti:
* {{servizio 1}}
* {{servizio 2}}

Confidenza di chiusura: {{x%}}
Data di chiusura: {{data}}

______________

Note in mail:

{{testo completo della mail}}

Regole finali:
- La primissima riga della risposta deve essere il nome azienda, non iniziare mai
  direttamente con "Gentile".
- Dentro "Note in mail" usa sempre i titoli "Analisi della Situazione Attuale",
  "Obiettivi", "Percorso Operativo" e "Conclusione".
- Inizia la mail con "Gentile {{Nome}},"
- Chiudi sempre con:

Un cordiale saluto,

Francesca

- Restituisci solo l'output finale, senza spiegazioni extra.
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
    known_data_dir: Optional[Path]
    default_seller_name: str
    sender_name: str
    sender_phone: str
    vat_research_enabled: bool
    vat_search_fallback_enabled: bool
    vat_research_timeout_seconds: float
    google_calendar_enabled: bool
    google_calendar_id: str
    google_calendar_lookup_before_minutes: int
    google_calendar_lookup_after_minutes: int
    google_calendar_next_meeting_days: int
    google_oauth_client_secrets_path: Optional[Path]
    google_oauth_token_path: Path
    google_sheets_enabled: bool
    google_sheet_id: str
    google_sheet_range: str
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


def env_str(name: str, default: str) -> str:
    return os.getenv(name, default).strip()


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
        known_data_dir=env_optional_path("KNOWN_DATA_DIR", "./DatiLead"),
        default_seller_name=env_str("DEFAULT_SELLER_NAME", "Marco") or "Marco",
        sender_name=env_str("SENDER_NAME", "Francesca") or "Francesca",
        sender_phone=env_str("SENDER_PHONE", ""),
        vat_research_enabled=env_bool("VAT_RESEARCH_ENABLED", True),
        vat_search_fallback_enabled=env_bool("VAT_SEARCH_FALLBACK_ENABLED", False),
        vat_research_timeout_seconds=env_float("VAT_RESEARCH_TIMEOUT_SECONDS", 8.0),
        google_calendar_enabled=env_bool("GOOGLE_CALENDAR_ENABLED", False),
        google_calendar_id=env_str("GOOGLE_CALENDAR_ID", "primary") or "primary",
        google_calendar_lookup_before_minutes=env_int("GOOGLE_CALENDAR_LOOKUP_BEFORE_MINUTES", 120),
        google_calendar_lookup_after_minutes=env_int("GOOGLE_CALENDAR_LOOKUP_AFTER_MINUTES", 30),
        google_calendar_next_meeting_days=env_int("GOOGLE_CALENDAR_NEXT_MEETING_DAYS", 45),
        google_oauth_client_secrets_path=env_optional_path(
            "GOOGLE_OAUTH_CLIENT_SECRETS", "./google_oauth_client_secret.json"
        ),
        google_oauth_token_path=env_path("GOOGLE_OAUTH_TOKEN", "./google_oauth_token.json"),
        google_sheets_enabled=env_bool("GOOGLE_SHEETS_ENABLED", False),
        google_sheet_id=env_str("GOOGLE_SHEET_ID", ""),
        google_sheet_range=env_str("GOOGLE_SHEET_RANGE", "FollowUp!A:Z") or "FollowUp!A:Z",
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
        if config.known_data_dir is not None:
            config.known_data_dir.mkdir(parents=True, exist_ok=True)
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


def find_known_data_context(
    config: AppConfig,
    lookup_stems: Sequence[str],
) -> Tuple[Optional[Path], str]:
    if config.known_data_dir is None or not config.known_data_dir.exists():
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
            path = config.known_data_dir / f"{stem}{suffix}"
            if path.exists() and path.is_file():
                return path, read_text(path, config.email_max_calendar_chars)

    return None, ""


def parse_reference_time_from_stem(stem: str) -> Optional[datetime]:
    for pattern in ("%Y-%m-%d %H-%M-%S", "%Y-%m-%d_%H-%M-%S", "%Y%m%d_%H%M%S"):
        try:
            parsed = datetime.strptime(stem, pattern)
            return parsed.astimezone()
        except ValueError:
            continue
    return None


def get_reference_time(audio_path: Optional[Path], transcript_path: Path) -> datetime:
    if audio_path is not None:
        parsed = parse_reference_time_from_stem(audio_path.stem)
        if parsed is not None:
            return parsed
        if audio_path.exists():
            return datetime.fromtimestamp(audio_path.stat().st_mtime).astimezone()
    return datetime.fromtimestamp(transcript_path.stat().st_mtime).astimezone()


def format_rfc3339(value: datetime) -> str:
    return value.astimezone().isoformat()


def get_google_credentials(config: AppConfig, scopes: Sequence[str]) -> Any:
    try:
        from google.auth.transport.requests import Request
        from google.oauth2.credentials import Credentials
        from google_auth_oauthlib.flow import InstalledAppFlow
    except ImportError as exc:
        raise ConfigError("Install Google dependencies with pip install -r requirements.txt") from exc

    creds = None
    if config.google_oauth_token_path.exists():
        creds = Credentials.from_authorized_user_file(str(config.google_oauth_token_path), list(scopes))

    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())

    if not creds or not creds.valid:
        if config.google_oauth_client_secrets_path is None or not config.google_oauth_client_secrets_path.exists():
            raise ConfigError("Missing GOOGLE_OAUTH_CLIENT_SECRETS for Google OAuth")
        flow = InstalledAppFlow.from_client_secrets_file(
            str(config.google_oauth_client_secrets_path),
            list(scopes),
        )
        creds = flow.run_local_server(port=0)

    config.google_oauth_token_path.write_text(creds.to_json(), encoding="utf-8")
    return creds


def build_google_service(config: AppConfig, service_name: str, version: str, scopes: Sequence[str]) -> Any:
    try:
        from googleapiclient.discovery import build
    except ImportError as exc:
        raise ConfigError("Install Google dependencies with pip install -r requirements.txt") from exc

    credentials = get_google_credentials(config, scopes)
    return build(service_name, version, credentials=credentials)


def event_datetime(value: Dict[str, str]) -> Optional[datetime]:
    raw_value = value.get("dateTime") or value.get("date")
    if not raw_value:
        return None
    if raw_value.endswith("Z"):
        raw_value = raw_value[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw_value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.astimezone()
    return parsed


def event_attendees(event: Dict[str, Any]) -> List[str]:
    attendees = []
    for attendee in event.get("attendees", []) or []:
        email = str(attendee.get("email") or "").strip()
        name = str(attendee.get("displayName") or "").strip()
        if email and name:
            attendees.append(f"{name} <{email}>")
        elif email:
            attendees.append(email)
        elif name:
            attendees.append(name)
    return attendees


def format_calendar_event(event: Dict[str, Any], prefix: str = "Evento") -> str:
    start = event_datetime(event.get("start", {}) or {})
    end = event_datetime(event.get("end", {}) or {})
    organizer = event.get("organizer", {}) or {}
    lines = [
        f"{prefix}: {event.get('summary') or 'Senza titolo'}",
        f"Inizio: {start.isoformat() if start else 'Non reperito'}",
        f"Fine: {end.isoformat() if end else 'Non reperito'}",
        f"Organizzatore: {organizer.get('displayName') or organizer.get('email') or 'Non reperito'}",
        f"Partecipanti: {', '.join(event_attendees(event)) or 'Non reperito'}",
    ]
    description = str(event.get("description") or "").strip()
    location = str(event.get("location") or "").strip()
    if location:
        lines.append(f"Luogo: {location}")
    if description:
        lines.extend(["Descrizione:", description])
    return "\n".join(lines)


def choose_best_calendar_event(events: Sequence[Dict[str, Any]], reference_time: datetime) -> Optional[Dict[str, Any]]:
    best_event = None
    best_distance = float("inf")
    reference_time = reference_time.astimezone()
    for event in events:
        start = event_datetime(event.get("start", {}) or {})
        end = event_datetime(event.get("end", {}) or {})
        if start and end and start <= reference_time <= end:
            return event
        candidate_time = end or start
        if candidate_time is None:
            continue
        distance = abs((candidate_time - reference_time).total_seconds())
        if distance < best_distance:
            best_distance = distance
            best_event = event
    return best_event


def attendee_email_set(event: Dict[str, Any]) -> Set[str]:
    emails = set()
    for attendee in event.get("attendees", []) or []:
        email = str(attendee.get("email") or "").strip().lower()
        if email:
            emails.add(email)
    return emails


def fetch_google_calendar_context(config: AppConfig, reference_time: datetime) -> str:
    if not config.google_calendar_enabled:
        return ""

    service = build_google_service(
        config,
        "calendar",
        "v3",
        ["https://www.googleapis.com/auth/calendar.readonly"],
    )
    time_min = reference_time - timedelta(minutes=config.google_calendar_lookup_before_minutes)
    time_max = reference_time + timedelta(minutes=config.google_calendar_lookup_after_minutes)
    response = (
        service.events()
        .list(
            calendarId=config.google_calendar_id,
            timeMin=format_rfc3339(time_min),
            timeMax=format_rfc3339(time_max),
            singleEvents=True,
            orderBy="startTime",
            maxResults=10,
        )
        .execute()
    )
    events = response.get("items", []) or []
    matched_event = choose_best_calendar_event(events, reference_time)
    if matched_event is None:
        return "Nessun evento Google Calendar trovato nella finestra configurata."

    parts = [format_calendar_event(matched_event, "Evento Google Calendar collegato")]
    attendee_emails = attendee_email_set(matched_event)
    if attendee_emails:
        future_time_max = reference_time + timedelta(days=config.google_calendar_next_meeting_days)
        future_response = (
            service.events()
            .list(
                calendarId=config.google_calendar_id,
                timeMin=format_rfc3339(reference_time + timedelta(minutes=1)),
                timeMax=format_rfc3339(future_time_max),
                singleEvents=True,
                orderBy="startTime",
                maxResults=20,
            )
            .execute()
        )
        future_events = []
        for event in future_response.get("items", []) or []:
            if attendee_emails.intersection(attendee_email_set(event)):
                future_events.append(event)
        if future_events:
            parts.append(format_calendar_event(future_events[0], "Possibile prossimo incontro gia fissato"))

    return "\n\n".join(parts)


def build_calendar_context(
    config: AppConfig,
    lookup_stems: Sequence[str],
    reference_time: datetime,
) -> Tuple[Optional[Path], str]:
    local_path, local_context = find_calendar_context(config, lookup_stems)
    google_context = ""
    try:
        google_context = fetch_google_calendar_context(config, reference_time)
    except Exception:
        logging.exception("Failed to fetch Google Calendar context")

    chunks = []
    if google_context:
        chunks.append("[GOOGLE_CALENDAR]\n" + google_context)
    if local_context:
        chunks.append(f"[LOCAL_CALENDAR_FILE: {local_path.name if local_path else 'unknown'}]\n{local_context}")
    return local_path, "\n\n".join(chunks)


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


def build_input_sections(
    transcript_text: str,
    transcript_path: Path,
    calendar_path: Optional[Path],
    calendar_context: str,
    known_data_path: Optional[Path],
    known_data_context: str,
    web_research_context: str,
    examples: Sequence[Tuple[Path, str]],
    config: AppConfig,
    extracted_facts: Optional[Dict[str, Any]] = None,
) -> str:
    calendar_label = calendar_path.name if calendar_path is not None else "non disponibile"
    calendar_text = calendar_context or "Nessun contesto Calendar trovato per questa trascrizione."
    known_data_label = known_data_path.name if known_data_path is not None else "non disponibile"
    known_data_text = known_data_context or "Nessun dato lead gia noto fornito."
    web_research_text = web_research_context or "Nessuna ricerca web disponibile."
    facts_text = (
        json.dumps(extracted_facts, ensure_ascii=False, indent=2)
        if extracted_facts is not None
        else "Fase di estrazione non ancora eseguita."
    )

    return "\n\n".join(
        [
            "Genera il follow-up post-call usando questi input separati.",
            f"FILE TRASCRIZIONE: {transcript_path.name}",
            f"CONTESTO CALENDAR: {calendar_label}",
            f"DATI LEAD: {known_data_label}",
            "[CALENDAR]",
            truncate_text(calendar_text, config.email_max_calendar_chars),
            "[TRANSCRIPT]",
            truncate_text(transcript_text, config.email_max_transcript_chars),
            "[KNOWN_DATA]",
            truncate_text(known_data_text, config.email_max_calendar_chars),
            "[WEB_RESEARCH]",
            truncate_text(web_research_text, config.email_max_calendar_chars),
            "[EXTRACTED_FACTS]",
            facts_text,
            "[EMAIL_EXAMPLES]",
            format_email_examples(examples),
            "[SENDER]",
            f"Nome mittente: {config.sender_name}",
            f"Telefono mittente: {config.sender_phone or 'Non reperito'}",
            f"Venditore di default se non emerge altro: {config.default_seller_name}",
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


def strip_html(raw_html: str) -> str:
    without_scripts = re.sub(r"(?is)<(script|style).*?>.*?</\1>", " ", raw_html)
    without_tags = re.sub(r"(?s)<[^>]+>", " ", without_scripts)
    return html.unescape(re.sub(r"\s+", " ", without_tags)).strip()


def extract_urls(source_text: str) -> Set[str]:
    return set(re.findall(r"https?://[^\s<>)\"']+", source_text, re.IGNORECASE))


def domain_from_email(email_value: str) -> Optional[str]:
    if "@" not in email_value:
        return None
    domain = email_value.rsplit("@", 1)[-1].strip().lower()
    if not domain or domain in {"gmail.com", "outlook.com", "hotmail.com", "icloud.com", "yahoo.com"}:
        return None
    return domain


def collect_candidate_domains(facts: Dict[str, Any], source_text: str) -> List[str]:
    domains: List[str] = []
    for email_value in extract_source_emails(source_text):
        domain = domain_from_email(email_value)
        if domain and domain not in domains:
            domains.append(domain)

    for url in extract_urls(source_text):
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        if domain.startswith("www."):
            domain = domain[4:]
        if domain and domain not in domains:
            domains.append(domain)
    return domains


def fetch_text_url(url: str, timeout_seconds: float) -> Optional[str]:
    try:
        response = requests.get(
            url,
            headers={"User-Agent": "Mozilla/5.0 (compatible; AxendFollowUpBot/1.0)"},
            timeout=timeout_seconds,
        )
        if response.status_code >= 400:
            return None
        return strip_html(response.text)
    except requests.RequestException:
        return None


def find_vat_candidates_with_sources(text: str, source_label: str) -> Dict[str, str]:
    candidates: Dict[str, str] = {}
    for match in re.finditer(r"\b(?:\d[\s.-]?){11}\b", text):
        digits = normalize_digits(match.group(0))
        if len(digits) == 11:
            candidates.setdefault(digits, source_label)
    return candidates


def research_vat_from_company_site(domains: Sequence[str], config: AppConfig) -> Dict[str, str]:
    paths = ["", "/", "/privacy", "/privacy-policy", "/contatti", "/contatto", "/contact", "/azienda"]
    candidates: Dict[str, str] = {}
    for domain in domains[:3]:
        for path in paths:
            url = f"https://{domain}{path}"
            text = fetch_text_url(url, config.vat_research_timeout_seconds)
            if not text and not path:
                text = fetch_text_url(f"http://{domain}", config.vat_research_timeout_seconds)
                url = f"http://{domain}"
            if not text:
                continue
            page_candidates = find_vat_candidates_with_sources(text, f"sito aziendale {domain}")
            candidates.update(page_candidates)
            if len(candidates) > 1:
                return candidates
        if candidates:
            return candidates
    return candidates


def research_vat_from_search(company_name: str, config: AppConfig) -> Dict[str, str]:
    if not company_name or company_name in {"Non reperito", "Da verificare"}:
        return {}
    query = quote_plus(f"{company_name} partita iva")
    search_url = f"https://duckduckgo.com/html/?q={query}"
    text = fetch_text_url(search_url, config.vat_research_timeout_seconds)
    if not text:
        return {}
    return find_vat_candidates_with_sources(text, "ricerca web DuckDuckGo")


def build_web_research_context(facts: Dict[str, Any], source_text: str, config: AppConfig) -> Tuple[str, Dict[str, Any]]:
    existing_vats = extract_source_vat_numbers(source_text)
    if len(existing_vats) == 1:
        vat_number = next(iter(existing_vats))
        payload = {
            "vat_status": "verified",
            "vat_number": vat_number,
            "vat_source": "Calendar/trascrizione/dati lead",
            "notes": ["P.IVA presente in una fonte prioritaria."],
        }
        return format_web_research_context(payload), payload
    if len(existing_vats) > 1:
        payload = {
            "vat_status": "conflict",
            "vat_number": "Da verificare",
            "vat_source": "Fonti prioritarie in conflitto",
            "notes": [f"P.IVA candidate trovate: {', '.join(sorted(existing_vats))}"],
        }
        return format_web_research_context(payload), payload

    if not config.vat_research_enabled:
        payload = {
            "vat_status": "not_found",
            "vat_number": "Non reperito",
            "vat_source": "Ricerca web disabilitata",
            "notes": ["P.IVA non presente nelle fonti prioritarie."],
        }
        return format_web_research_context(payload), payload

    candidates: Dict[str, str] = {}
    domains = collect_candidate_domains(facts, source_text)
    candidates.update(research_vat_from_company_site(domains, config))
    if not candidates and config.vat_search_fallback_enabled:
        candidates.update(research_vat_from_search(str(facts.get("company_name") or ""), config))

    if len(candidates) == 1:
        vat_number, vat_source = next(iter(candidates.items()))
        payload = {
            "vat_status": "verified",
            "vat_number": vat_number,
            "vat_source": vat_source,
            "notes": ["P.IVA recuperata da ricerca esterna."],
        }
    elif len(candidates) > 1:
        payload = {
            "vat_status": "conflict",
            "vat_number": "Da verificare",
            "vat_source": "Ricerca esterna in conflitto",
            "notes": [f"P.IVA candidate trovate: {', '.join(sorted(candidates))}"],
        }
    else:
        payload = {
            "vat_status": "not_found",
            "vat_number": "Non reperito",
            "vat_source": "Non reperito",
            "notes": ["P.IVA non recuperata da fonti prioritarie o ricerca esterna."],
        }
    return format_web_research_context(payload), payload


def format_web_research_context(payload: Dict[str, Any]) -> str:
    notes = payload.get("notes") or []
    return "\n".join(
        [
            f"vat_status: {payload.get('vat_status', 'not_found')}",
            f"vat_number: {payload.get('vat_number', 'Non reperito')}",
            f"vat_source: {payload.get('vat_source', 'Non reperito')}",
            "notes:",
            *[f"- {note}" for note in notes],
        ]
    )


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


FACT_DEFAULTS: Dict[str, Any] = {
    "company_name": "Non reperito",
    "seller_name": "Non reperito",
    "contact_name": "Non reperito",
    "contact_first_name": "Non reperito",
    "phone": "Non reperito",
    "email": "Non reperito",
    "vat_number": "Non reperito",
    "vat_source": "Non reperito",
    "proposed_services": [],
    "close_confidence": "Non reperito",
    "close_date": "Non reperito",
    "next_meeting_date": "Non reperito",
    "next_meeting_text": "Non reperito",
    "needs": [],
    "current_issues": [],
    "goals": [],
    "current_situation": "Non reperito",
    "decision_makers": [],
    "attachments_to_send": [],
    "missing_or_to_verify": [],
}


def normalize_fact_value(value: Any) -> str:
    clean_value = str(value or "").strip()
    if not clean_value or clean_value.lower() in {"none", "null", "n/a"}:
        return "Non reperito"
    return clean_value


def normalize_facts(raw_facts: Dict[str, Any], config: AppConfig) -> Dict[str, Any]:
    facts: Dict[str, Any] = {}
    for key, default in FACT_DEFAULTS.items():
        value = raw_facts.get(key, default)
        if isinstance(default, list):
            facts[key] = coerce_string_list(value)
        else:
            facts[key] = normalize_fact_value(value)

    if facts["seller_name"] == "Non reperito" and config.default_seller_name:
        facts["seller_name"] = config.default_seller_name
    if facts["contact_first_name"] in {"Non reperito", "Da verificare"} and facts["contact_name"] not in {
        "Non reperito",
        "Da verificare",
    }:
        facts["contact_first_name"] = facts["contact_name"].split()[0]
    return facts


def infer_services_from_text(facts: Dict[str, Any], source_text: str) -> Dict[str, Any]:
    services = coerce_string_list(facts.get("proposed_services"))
    service_keys = {service.lower() for service in services}
    lower_source = source_text.lower()

    if "analisi" in lower_source and not any("analisi" in service for service in service_keys):
        services.append("Analisi Basic")
    telesetting_terms = ("telesetting", "commerciale", "chiamate", "appuntamenti", "prenotare")
    if any(term in lower_source for term in telesetting_terms) and not any(
        "telesetting" in service for service in service_keys
    ):
        services.append("Telesetting Advanced")

    facts["proposed_services"] = services
    return facts


def enforce_source_backed_fact_dates(facts: Dict[str, Any], source_text: str) -> Dict[str, Any]:
    for key in ("next_meeting_date", "close_date"):
        value = str(facts.get(key) or "")
        if value in {"Non reperito", "Da verificare"}:
            continue
        years = re.findall(r"\b(?:19|20)\d{2}\b", value)
        if years and not all(year in source_text for year in years):
            facts[key] = "Da verificare"
            missing = coerce_string_list(facts.get("missing_or_to_verify"))
            label = "Data prossimo incontro" if key == "next_meeting_date" else "Data di chiusura"
            if label not in missing:
                missing.append(label)
            facts["missing_or_to_verify"] = missing
    return facts


def enforce_source_backed_contact_facts(facts: Dict[str, Any], source_text: str) -> Dict[str, Any]:
    missing = coerce_string_list(facts.get("missing_or_to_verify"))
    source_emails = extract_source_emails(source_text)
    email = str(facts.get("email") or "").strip().lower()
    if email and email not in {"non reperito", "da verificare"} and email not in source_emails:
        facts["email"] = "Non reperito"
        if "Email referente" not in missing:
            missing.append("Email referente")
    elif email in {"", "non reperito"} and len(source_emails) == 1:
        facts["email"] = next(iter(source_emails))

    phone = str(facts.get("phone") or "").strip()
    if phone and phone not in {"Non reperito", "Da verificare"} and not source_contains_digit_value(phone, source_text):
        facts["phone"] = "Non reperito"
        if "Telefono referente" not in missing:
            missing.append("Telefono referente")

    vat_number = normalize_digits(facts.get("vat_number"))
    source_vats = extract_source_vat_numbers(source_text)
    if vat_number and facts.get("vat_number") not in {"Non reperito", "Da verificare"} and vat_number not in source_vats:
        facts["vat_number"] = "Non reperito"
        facts["vat_source"] = "Non reperito"
        if "Partita IVA" not in missing:
            missing.append("Partita IVA")

    facts["missing_or_to_verify"] = missing
    return facts


def apply_verified_vat(facts: Dict[str, Any], vat_payload: Dict[str, Any]) -> Dict[str, Any]:
    vat_status = str(vat_payload.get("vat_status") or "not_found")
    if vat_status == "verified":
        facts["vat_number"] = normalize_fact_value(vat_payload.get("vat_number"))
        facts["vat_source"] = normalize_fact_value(vat_payload.get("vat_source"))
    elif vat_status == "conflict":
        facts["vat_number"] = "Da verificare"
        facts["vat_source"] = normalize_fact_value(vat_payload.get("vat_source"))
    elif facts.get("vat_number") not in {"Non reperito", "Da verificare"}:
        facts["vat_source"] = facts.get("vat_source") or "Da verificare"
    else:
        facts["vat_number"] = "Non reperito"
        facts["vat_source"] = "Non reperito"

    missing = coerce_string_list(facts.get("missing_or_to_verify"))
    if facts["vat_number"] in {"Non reperito", "Da verificare"} and "Partita IVA" not in missing:
        missing.append("Partita IVA")
    facts["missing_or_to_verify"] = missing
    return facts


def generate_call_facts(
    transcript_text: str,
    transcript_path: Path,
    calendar_path: Optional[Path],
    calendar_context: str,
    known_data_path: Optional[Path],
    known_data_context: str,
    examples: Sequence[Tuple[Path, str]],
    config: AppConfig,
) -> Dict[str, Any]:
    logging.info("Extracting sales call facts with OpenAI model %s", config.email_model)
    client = OpenAI(api_key=config.openai_api_key)
    response = client.responses.create(
        model=config.email_model,
        input=[
            {
                "role": "system",
                "content": [{"type": "input_text", "text": FACT_EXTRACTION_SYSTEM_PROMPT}],
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": build_input_sections(
                            transcript_text=transcript_text,
                            transcript_path=transcript_path,
                            calendar_path=calendar_path,
                            calendar_context=calendar_context,
                            known_data_path=known_data_path,
                            known_data_context=known_data_context,
                            web_research_context="",
                            examples=examples,
                            config=config,
                        ),
                    }
                ],
            },
        ],
        temperature=0,
        max_output_tokens=config.email_max_output_tokens,
    )
    facts_text = extract_openai_text(response)
    if not facts_text:
        raise RuntimeError("OpenAI returned empty call facts")
    try:
        raw_facts = parse_json_object(facts_text)
    except Exception as exc:
        raise RuntimeError(f"OpenAI returned invalid facts JSON: {facts_text[:500]}") from exc
    return normalize_facts(raw_facts, config)


def generate_final_email_output(
    transcript_text: str,
    transcript_path: Path,
    calendar_path: Optional[Path],
    calendar_context: str,
    known_data_path: Optional[Path],
    known_data_context: str,
    web_research_context: str,
    examples: Sequence[Tuple[Path, str]],
    facts: Dict[str, Any],
    config: AppConfig,
) -> str:
    logging.info("Generating final sales email output with OpenAI model %s", config.email_model)
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
                        "text": build_input_sections(
                            transcript_text=transcript_text,
                            transcript_path=transcript_path,
                            calendar_path=calendar_path,
                            calendar_context=calendar_context,
                            known_data_path=known_data_path,
                            known_data_context=known_data_context,
                            web_research_context=web_research_context,
                            examples=examples,
                            config=config,
                            extracted_facts=facts,
                        ),
                    }
                ],
            },
        ],
        temperature=0.2,
        max_output_tokens=config.email_max_output_tokens,
    )
    final_output = extract_openai_text(response)
    if not final_output:
        raise RuntimeError("OpenAI returned an empty email output")
    return final_output.strip() + "\n"


def format_services_for_output(services: Sequence[str]) -> str:
    clean_services = [service for service in services if service]
    if not clean_services:
        return "* Non reperito"
    return "\n".join(f"* {service}" for service in clean_services)


def piva_source_line(facts: Dict[str, Any], vat_payload: Dict[str, Any]) -> str:
    if str(vat_payload.get("vat_status") or "") != "verified":
        return ""
    source = normalize_fact_value(facts.get("vat_source"))
    if source in {"Non reperito", "Da verificare"}:
        return ""
    company = normalize_fact_value(facts.get("company_name"))
    return f"\nFonte P.IVA {company}: {source}\n"


def extract_body_from_model_output(output: str) -> str:
    stripped = output.strip()
    if "Note in mail:" in stripped:
        candidate = stripped.split("Note in mail:", 1)[1].strip()
        if "______________" in candidate:
            candidate = candidate.split("______________", 1)[1].strip()
        if not candidate.startswith("Gentile ") and "Gentile " in candidate:
            candidate = candidate[candidate.rfind("Gentile ") :].strip()
        return candidate
    if "______________" in stripped:
        after_separator = stripped.split("______________", 1)[1].strip()
        gentile_index = after_separator.find("Gentile ")
        if gentile_index >= 0:
            return after_separator[gentile_index:].strip()
    return stripped


def has_reliable_next_meeting(facts: Dict[str, Any]) -> bool:
    meeting_date = normalize_fact_value(facts.get("next_meeting_date"))
    meeting_text = normalize_fact_value(facts.get("next_meeting_text"))
    combined = f"{meeting_date} {meeting_text}".lower()
    if "da verificare" in combined or "non reperito" in combined:
        return False
    return bool(re.search(r"\d", combined)) and any(
        month in combined
        for month in (
            "gennaio",
            "febbraio",
            "marzo",
            "aprile",
            "maggio",
            "giugno",
            "luglio",
            "agosto",
            "settembre",
            "ottobre",
            "novembre",
            "dicembre",
            "/",
            "-",
        )
    )


def remove_unreliable_followup_paragraphs(body: str, facts: Dict[str, Any]) -> str:
    if has_reliable_next_meeting(facts):
        return body
    paragraphs = re.split(r"\n\s*\n", body.strip())
    risky_terms = (
        "prossimo incontro",
        "incontro fissato",
        "incontro di aggiornamento",
        "ci sentiremo",
        "confermo il nostro incontro",
        "confermo il prossimo",
    )
    kept = []
    for paragraph in paragraphs:
        lower_paragraph = paragraph.lower()
        if any(term in lower_paragraph for term in risky_terms):
            continue
        kept.append(paragraph)
    return "\n\n".join(kept).strip()


def ensure_final_output_format(final_output: str, facts: Dict[str, Any], vat_payload: Dict[str, Any]) -> str:
    stripped = final_output.strip()
    company = normalize_fact_value(facts.get("company_name"))
    seller = normalize_fact_value(facts.get("seller_name"))
    contact = normalize_fact_value(facts.get("contact_name"))
    phone = normalize_fact_value(facts.get("phone"))
    email = normalize_fact_value(facts.get("email"))
    vat_number = normalize_fact_value(facts.get("vat_number"))
    confidence = normalize_fact_value(facts.get("close_confidence"))
    close_date = normalize_fact_value(facts.get("close_date"))
    services = format_services_for_output(coerce_string_list(facts.get("proposed_services")))

    body = extract_body_from_model_output(stripped)
    body = remove_unreliable_followup_paragraphs(body, facts)
    if not body.startswith("Gentile "):
        first_name = normalize_fact_value(facts.get("contact_first_name"))
        if first_name in {"Non reperito", "Da verificare"}:
            first_name = contact
        body = f"Gentile {first_name},\n\n{body}"
    if "Un cordiale saluto" not in body:
        body = body.rstrip() + "\n\nUn cordiale saluto,\n\nFrancesca"

    output = "\n".join(
        [
            company,
            "",
            f"Venditore: {seller}",
            "",
            f"Riferimento: {contact}",
            "",
            f"Tel: {phone}",
            "",
            f"Mail: {email}",
            "",
            f"Partita IVA: {vat_number}",
            "",
            "Servizi proposti:",
            services,
            "",
            f"Confidenza di chiusura: {confidence}",
            f"Data di chiusura: {close_date}",
            "",
            "______________",
            "",
            "Note in mail:",
            "",
            body.strip(),
        ]
    )
    source_line = piva_source_line(facts, vat_payload)
    if source_line:
        output = output.rstrip() + source_line
    return output.strip() + "\n"


def generate_sales_email_draft(
    transcript_text: str,
    transcript_path: Path,
    calendar_path: Optional[Path],
    calendar_context: str,
    known_data_path: Optional[Path],
    known_data_context: str,
    examples: Sequence[Tuple[Path, str]],
    config: AppConfig,
) -> Dict[str, Any]:
    source_text = "\n\n".join([transcript_text, calendar_context, known_data_context])
    facts = generate_call_facts(
        transcript_text=transcript_text,
        transcript_path=transcript_path,
        calendar_path=calendar_path,
        calendar_context=calendar_context,
        known_data_path=known_data_path,
        known_data_context=known_data_context,
        examples=examples,
        config=config,
    )
    facts = enforce_source_backed_contact_facts(facts, source_text)
    facts = infer_services_from_text(facts, source_text)
    facts = enforce_source_backed_fact_dates(facts, source_text)
    web_research_context, vat_payload = build_web_research_context(facts, source_text, config)
    facts = apply_verified_vat(facts, vat_payload)
    final_output = generate_final_email_output(
        transcript_text=transcript_text,
        transcript_path=transcript_path,
        calendar_path=calendar_path,
        calendar_context=calendar_context,
        known_data_path=known_data_path,
        known_data_context=known_data_context,
        web_research_context=web_research_context,
        examples=examples,
        facts=facts,
        config=config,
    )
    final_output = ensure_final_output_format(final_output, facts, vat_payload)

    draft = {
        "final_output": final_output,
        "facts": facts,
        "web_research": vat_payload,
        "source_notes": [
            "Calendar Google o file locale usato se disponibile.",
            "Trascrizione completa usata come fonte principale della call.",
            "P.IVA verificata solo se presente nelle fonti o recuperata da ricerca esterna.",
        ],
        "missing_fields": coerce_string_list(facts.get("missing_or_to_verify")),
    }
    draft["source_transcript_file"] = transcript_path.name
    draft["source_calendar_file"] = calendar_path.name if calendar_path is not None else None
    draft["source_known_data_file"] = known_data_path.name if known_data_path is not None else None
    draft["examples_used"] = [path.name for path, _ in examples]
    return draft


def sheet_row_from_draft(draft: Dict[str, Any], draft_path: Path, json_path: Path) -> List[str]:
    facts = draft.get("facts") if isinstance(draft.get("facts"), dict) else {}
    web_research = draft.get("web_research") if isinstance(draft.get("web_research"), dict) else {}
    return [
        datetime.now().astimezone().isoformat(timespec="seconds"),
        str(draft.get("source_transcript_file") or ""),
        str(facts.get("company_name") or "Non reperito"),
        str(facts.get("seller_name") or "Non reperito"),
        str(facts.get("contact_name") or "Non reperito"),
        str(facts.get("phone") or "Non reperito"),
        str(facts.get("email") or "Non reperito"),
        str(facts.get("vat_number") or "Non reperito"),
        str(web_research.get("vat_status") or "not_found"),
        str(facts.get("vat_source") or "Non reperito"),
        "\n".join(coerce_string_list(facts.get("proposed_services"))),
        str(facts.get("close_confidence") or "Non reperito"),
        str(facts.get("close_date") or "Non reperito"),
        str(facts.get("next_meeting_date") or "Non reperito"),
        str(facts.get("next_meeting_text") or "Non reperito"),
        "\n".join(coerce_string_list(facts.get("current_issues"))),
        "\n".join(coerce_string_list(facts.get("goals"))),
        "\n".join(coerce_string_list(facts.get("missing_or_to_verify"))),
        str(draft_path),
        str(json_path),
        str(draft.get("final_output") or ""),
    ]


def append_draft_to_google_sheet(draft: Dict[str, Any], draft_path: Path, json_path: Path, config: AppConfig) -> None:
    if not config.google_sheets_enabled:
        return
    if not config.google_sheet_id:
        logging.warning("GOOGLE_SHEETS_ENABLED=true but GOOGLE_SHEET_ID is empty")
        return

    service = build_google_service(
        config,
        "sheets",
        "v4",
        ["https://www.googleapis.com/auth/spreadsheets"],
    )
    body = {"values": [sheet_row_from_draft(draft, draft_path, json_path)]}
    (
        service.spreadsheets()
        .values()
        .append(
            spreadsheetId=config.google_sheet_id,
            range=config.google_sheet_range,
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body=body,
        )
        .execute()
    )
    logging.info("Appended email draft to Google Sheet range %s", config.google_sheet_range)


def save_email_outputs(
    draft: Dict[str, Any],
    title: str,
    config: AppConfig,
) -> Tuple[Path, Path]:
    config.email_drafts_dir.mkdir(parents=True, exist_ok=True)
    safe_title = sanitize_filename(title)
    stem = f"{safe_title}_email"
    draft_path = config.email_drafts_dir / f"{stem}.txt"
    json_path = config.email_drafts_dir / f"{stem}.json"
    counter = 2
    while draft_path.exists() or json_path.exists():
        draft_path = config.email_drafts_dir / f"{stem}_{counter}.txt"
        json_path = config.email_drafts_dir / f"{stem}_{counter}.json"
        counter += 1

    draft_path.write_text(str(draft.get("final_output") or "").strip() + "\n", encoding="utf-8")
    json_path.write_text(json.dumps(draft, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    try:
        append_draft_to_google_sheet(draft, draft_path, json_path, config)
    except Exception:
        logging.exception("Failed to append email draft to Google Sheet")
    logging.info("Saved email draft: %s", draft_path)
    logging.info("Saved structured payload draft: %s", json_path)
    return draft_path, json_path


def generate_and_save_email_draft(
    transcript_path: Path,
    transcript_text: str,
    title: str,
    config: AppConfig,
    extra_calendar_lookup_stems: Sequence[str] = (),
    audio_path: Optional[Path] = None,
    force: bool = False,
) -> Optional[Tuple[Path, Path]]:
    if not config.email_drafts_enabled and not force:
        return None

    lookup_stems = [transcript_path.stem, title, *extra_calendar_lookup_stems]
    reference_time = get_reference_time(audio_path, transcript_path)
    calendar_path, calendar_context = build_calendar_context(config, lookup_stems, reference_time)
    known_data_path, known_data_context = find_known_data_context(config, lookup_stems)
    examples = load_email_examples(config)
    draft = generate_sales_email_draft(
        transcript_text=transcript_text,
        transcript_path=transcript_path,
        calendar_path=calendar_path,
        calendar_context=calendar_context,
        known_data_path=known_data_path,
        known_data_context=known_data_context,
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
            audio_path=audio_path,
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
