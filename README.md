# Trascrittore Auto

Automazione locale per:

1. guardare una cartella dove OBS salva file `.mp3`;
2. aspettare che il file sia completo;
3. inviare l'audio ad AssemblyAI con speaker diarization;
4. generare un nome file con OpenAI;
5. salvare la trascrizione `.txt`;
6. generare una bozza email post-call e un payload `.json` utile per il CRM;
7. archiviare o eliminare l'audio originale.

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
- `./BozzeEmail` per bozze email `.md` e payload `.json`
- `./EsempiEmail` per esempi email da imitare come tono/struttura
- `./EventiCalendar` per contesto Calendar opzionale
- `./Archivio` per gli audio completati

## Avvio

```bash
source .venv/bin/activate
python transcrittore_auto.py
```

Lascia il terminale aperto. Ogni nuovo `.mp3` in `WATCH_DIR` verra processato in automatico.

## Bozze email post-call

Quando `EMAIL_DRAFTS_ENABLED=true`, dopo ogni trascrizione lo script genera:

- `BozzeEmail/Nome_Cliente_email.md`: email pronta da rivedere e inviare;
- `BozzeEmail/Nome_Cliente_email.json`: dati strutturati pensati per il futuro caricamento nel CRM.

La mail viene costruita dalla trascrizione, dal contesto Calendar se presente, e dagli esempi email salvati in `EsempiEmail`.
Il corpo della mail non contiene citazioni inline: le note sulle fonti vengono messe solo in fondo alla bozza.

Per usare esempi reali, salva una mail per file dentro `EsempiEmail`, per esempio:

```text
EsempiEmail/esempio_01.txt
EsempiEmail/esempio_02.txt
```

Per aggiungere dati Calendar senza ancora collegare le API Google, salva un file con lo stesso nome della trascrizione dentro `EventiCalendar`, in formato `.txt`, `.md` o `.json`.
Esempio:

```text
EventiCalendar/Veronica_Teka_Sfera.txt
```

Il file puo contenere dati come nome contatto, azienda, email, partita IVA, data/ora dell'evento e note dell'invito.

Per generare una bozza da una trascrizione gia esistente:

```bash
source .venv/bin/activate
python transcrittore_auto.py --draft-email-from-transcript ./Trascrizioni/Nome_Cliente.txt
```

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

- `FILE_STABILITY_INTERVAL_SECONDS`, `FILE_STABILITY_REQUIRED_CHECKS` e `FILE_QUIET_SECONDS` controllano quanto lo script aspetta prima di considerare chiuso il file scritto da OBS.
- `DELETE_AUDIO_AFTER_PROCESSING=false` sposta l'audio in `ARCHIVE_DIR`.
- `DELETE_AUDIO_AFTER_PROCESSING=true` elimina l'audio dopo aver salvato la trascrizione.
- `ASSEMBLYAI_SPEAKERS_EXPECTED=2` puo migliorare la diarizzazione se le call hanno sempre due speaker. Lascialo vuoto se non sei certo.
- `EMAIL_DRAFTS_DIR` puo puntare a una cartella locale sincronizzata con Google Drive.
- `EMAIL_EXAMPLES_DIR` e `EventiCalendar` sono intenzionalmente ignorate da Git per evitare di pubblicare dati clienti.
