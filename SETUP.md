# Job Monitor - setup (about 15 minutes)

Checks new-grad job lists every 30 minutes, filters them by your rules,
adds new jobs to your Notion "Jobs" database, and sends phone alerts.

## 1. Phone alerts (ntfy)
1. Install the **ntfy** app (iPhone or Android).
2. Tap **+** and subscribe to a long random topic name, e.g. `syimyk-jobs-7f3k9q2x`.
   Anyone who knows this name can read the alerts, so keep it random and private.

## 2. Notion access for the script
1. Go to https://www.notion.so/profile/integrations -> **New integration**.
   Name: `Job Monitor`, type: Internal. Copy the **secret** (starts with `ntn_`).
2. Open your **Job Search 2026** page in Notion -> `...` menu -> **Connections** -> add `Job Monitor`.

## 3. GitHub repository
1. Create a new repository named `job-monitor`.
   - **Public repo**: GitHub Actions minutes are free. Nothing personal is stored in the code.
   - **Private repo**: fine. Every 30 minutes uses about 1,450 of the 2,000 free minutes/month.
     To go faster later, make the repo public or use the GitHub Student Developer Pack.
2. Upload all files from this folder (keep the `.github/workflows/` and `state/` folders).
3. Repo **Settings -> Secrets and variables -> Actions -> New repository secret**:
   - `NOTION_TOKEN` = your Notion secret
   - `NTFY_TOPIC` = your ntfy topic name
4. **Actions** tab -> enable workflows -> open **job-monitor** -> **Run workflow**.

## What the first run does
- Imports current matching openings from the last 21 days, labeled **Existing at setup**.
  No phone alerts for these.
- Adds at most 150 rows per run; anything left over is added on the next run.
- After that, every new matching job is labeled **New**.
  Backend / Platform / Full Stack roles -> instant phone alert.
  Other roles -> grouped digest. No alerts between midnight and 7 AM Central.
- If a source or Notion fails, you get a "Job monitor problem" alert.

## Change settings
Edit `config.json`: role keywords, excluded words, quiet hours, and company boards
(`"greenhouse": ["stripe"]`, `"lever": [...]`, `"ashby": [...]`).
