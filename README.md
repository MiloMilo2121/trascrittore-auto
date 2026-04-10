# Trascrittore Auto

Automazione locale per:

1. guardare una cartella dove OBS salva file `.mp3`;
2. aspettare che il file sia completo;
3. inviare l'audio ad AssemblyAI con speaker diarization;
4. generare un nome file con OpenAI;
5. salvare la trascrizione `.txt`;
6. archiviare o eliminare l'audio originale.

## Requisiti

- Python 3.9+
- Una API key AssemblyAI
- Una API key OpenAI
- OBS configurato per salvare audio `.mp3` nella cartella indicata da `WATCH_DIR`

## Installazione

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Apri `.env` e inserisci:

```bash
ASSEMBLYAI_API_KEY=...
OPENAI_API_KEY=...
```

Di default lo script usa queste cartelle, create automaticamente se mancanti:

- `./RECS` per gli audio OBS
- `./Trascrizioni` per i file `.txt`
- `./Archivio` per gli audio completati

## Avvio

```bash
source .venv/bin/activate
python transcrittore_auto.py
```

Lascia il terminale aperto. Ogni nuovo `.mp3` in `WATCH_DIR` verra processato in automatico.

## Test su un singolo file

```bash
source .venv/bin/activate
python transcrittore_auto.py --once ./RECS/test.mp3
```

## Pacchetti pip

```bash
pip install -r requirements.txt
```

Pacchetti usati:

- `watchdog`
- `requests`
- `urllib3<2`
- `openai`
- `python-dotenv`

## Note operative

- `FILE_STABILITY_INTERVAL_SECONDS` e `FILE_STABILITY_REQUIRED_CHECKS` controllano quanto lo script aspetta prima di considerare chiuso il file scritto da OBS.
- `DELETE_AUDIO_AFTER_PROCESSING=false` sposta l'audio in `ARCHIVE_DIR`.
- `DELETE_AUDIO_AFTER_PROCESSING=true` elimina l'audio dopo aver salvato la trascrizione.
- `ASSEMBLYAI_SPEAKERS_EXPECTED=2` puo migliorare la diarizzazione se le call hanno sempre due speaker. Lascialo vuoto se non sei certo.
