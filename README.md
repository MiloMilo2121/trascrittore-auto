# Trascrittore Auto

Automazione locale per:

1. guardare una cartella dove OBS salva file `.mp3`;
2. aspettare che il file sia completo;
3. inviare l'audio ad AssemblyAI con speaker diarization;
4. generare un nome file con OpenAI;
5. salvare la trascrizione `.txt`;
6. recuperare il contesto Calendar o i dati lead disponibili;
7. verificare la P.IVA se non e gia presente in fonti affidabili;
8. generare una bozza email post-call e un payload `.json` utile per CRM e Google Sheet;
9. archiviare o eliminare l'audio originale.

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
- `./BozzeEmail` per bozze email `.txt` e payload `.json`
- `./EsempiEmail` per esempi email da imitare come tono/struttura
- `./EventiCalendar` per contesto Calendar opzionale
- `./DatiLead` per dati prospect opzionali gia noti
- `./Archivio` per gli audio completati

## Avvio

```bash
source .venv/bin/activate
python transcrittore_auto.py
```

Lascia il terminale aperto. Ogni nuovo `.mp3` in `WATCH_DIR` verra processato in automatico.

## Bozze email post-call

Quando `EMAIL_DRAFTS_ENABLED=true`, dopo ogni trascrizione lo script genera:

- `BozzeEmail/Nome_Cliente_email.txt`: output finale nel formato Francesca, pronto da rivedere e inviare;
- `BozzeEmail/Nome_Cliente_email.json`: dati strutturati pensati per CRM e Google Sheet.

La mail viene costruita in due fasi: prima l'AI estrae e normalizza i dati commerciali, poi scrive l'output finale con tono Francesca, "Lei" obbligatorio e nessun link nel corpo.
La P.IVA viene usata solo se presente in Calendar/trascrizione/dati lead o se recuperata da un dominio presente nelle fonti; se non e affidabile resta `Non reperito` o `Da verificare`.

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

Puoi aggiungere dati lead gia noti nello stesso modo dentro `DatiLead`, ad esempio:

```text
DatiLead/Veronica_Teka_Sfera.txt
```

Quando saranno disponibili OAuth e ID del foglio, abilita Google Calendar e Google Sheets in `.env`:

```bash
GOOGLE_OAUTH_CLIENT_SECRETS=./google_oauth_client_secret.json
GOOGLE_OAUTH_TOKEN=./google_oauth_token.json
GOOGLE_OAUTH_LOCAL_PORT=8080
GOOGLE_CALENDAR_ENABLED=true
GOOGLE_CALENDAR_ID=primary
GOOGLE_SHEETS_ENABLED=true
GOOGLE_SHEET_ID=...
GOOGLE_SHEET_RANGE=FollowUp!A:Z
```

Il primo avvio OAuth va fatto manualmente da terminale con `--draft-email-from-transcript`, cosi puo aprire il browser e salvare il token locale in `google_oauth_token.json`. Se Google mostra `redirect_uri_mismatch`, nella console Google Cloud dell'OAuth client aggiungi l'URI di redirect autorizzato `http://localhost:8080/` oppure cambia `GOOGLE_OAUTH_LOCAL_PORT` e autorizza lo stesso URI con quella porta. Dopo il token, il LaunchAgent puo usare Calendar e Sheets in automatico.

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
- `google-api-python-client`
- `google-auth`
- `google-auth-oauthlib`

## Note operative

- `FILE_STABILITY_INTERVAL_SECONDS`, `FILE_STABILITY_REQUIRED_CHECKS` e `FILE_QUIET_SECONDS` controllano quanto lo script aspetta prima di considerare chiuso il file scritto da OBS.
- `DELETE_AUDIO_AFTER_PROCESSING=false` sposta l'audio in `ARCHIVE_DIR`.
- `DELETE_AUDIO_AFTER_PROCESSING=true` elimina l'audio dopo aver salvato la trascrizione.
- `ASSEMBLYAI_SPEAKERS_EXPECTED=2` puo migliorare la diarizzazione se le call hanno sempre due speaker. Lascialo vuoto se non sei certo.
- `EMAIL_DRAFTS_DIR` puo puntare a una cartella locale sincronizzata con Google Drive.
- `EMAIL_EXAMPLES_DIR`, `EventiCalendar` e `DatiLead` sono intenzionalmente ignorate da Git per evitare di pubblicare dati clienti.
- `GOOGLE_SHEET_RANGE` punta sempre allo stesso foglio operativo; ogni bozza aggiunge una riga con dati normalizzati, file locali e testo mail completo.
- `VAT_SEARCH_FALLBACK_ENABLED=false` evita di prendere P.IVA da ricerche generiche quando manca un sito/dominio affidabile nelle fonti.
