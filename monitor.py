"""Job monitor: fetch new-grad jobs, filter, dedupe, write to Notion, push alerts.

Code does fetching, filtering, dedupe and state. Claude does fit scoring and writing later.
Run:  python monitor.py            (normal)
      python monitor.py --dry-run  (no Notion writes, no alerts, no state save)
Env:  NOTION_TOKEN, NTFY_TOPIC
"""
import json, os, re, sys, time, urllib.request, urllib.error
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

HERE = os.path.dirname(os.path.abspath(__file__))
CFG = json.load(open(os.path.join(HERE, "config.json")))
STATE_PATH = os.path.join(HERE, "state", "state.json")
RUNLOG_PATH = os.path.join(HERE, "state", "runs.jsonl")
DRY = "--dry-run" in sys.argv
TZ = ZoneInfo(CFG["timezone"])

US_STATES = set("AL AK AZ AR CA CO CT DE FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS MO MT NE NV NH NJ NM NY NC ND OH OK OR PA RI SC SD TN TX UT VT VA WA WV WI WY DC".split())
NON_US = ["canada", "uk", "united kingdom", "india", "germany", "london", "toronto", "vancouver", "mexico",
          "ireland", "poland", "singapore", "israel", "netherlands", "france", "spain", "japan", "china", "brazil", "australia",
          ", on", ", bc", ", qc", ", ab"]


# ---------- helpers ----------
def http(url, method="GET", data=None, headers=None, retries=3):
    """HTTP with bounded retries and backoff. Returns (status, body_bytes)."""
    headers = headers or {}
    body = json.dumps(data).encode() if isinstance(data, (dict, list)) else data
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, data=body, method=method, headers=headers)
            with urllib.request.urlopen(req, timeout=60) as r:
                return r.status, r.read()
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503, 504) and attempt < retries - 1:
                time.sleep(float(e.headers.get("Retry-After", 2 ** (attempt + 1))))
                continue
            return e.code, e.read()
        except Exception:
            if attempt < retries - 1:
                time.sleep(2 ** (attempt + 1))
                continue
            raise


def norm(s):
    return re.sub(r"[^a-z0-9]+", " ", (s or "").lower()).strip()


def has_word(text, words):
    t = " " + norm(text) + " "
    return [w for w in words if " " + norm(w) + " " in t]


def is_us(locations):
    if not locations:
        return "unknown"
    ok = False
    for loc in locations:
        l = loc.lower()
        if any(n in l for n in NON_US):
            continue
        m = re.search(r",\s*([A-Z]{2})\b", loc)
        if (m and m.group(1) in US_STATES) or "remote" in l or "united states" in l or "usa" in l or l.endswith(" us"):
            ok = True
    return "yes" if ok else "no"


def role_type(title):
    for rt, words in CFG["role_rules"].items():
        if has_word(title, words):
            return rt
    return None


def load_state():
    if os.path.exists(STATE_PATH):
        return json.load(open(STATE_PATH))
    return {"bootstrapped": False, "seen": {}, "pending_digest": [], "sources": {}}


def save_state(state):
    if DRY:
        return
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    tmp = STATE_PATH + ".tmp"
    json.dump(state, open(tmp, "w"), indent=0, sort_keys=True)
    os.replace(tmp, STATE_PATH)  # atomic write = safe checkpoint


# ---------- sources ----------
def fetch_simplify():
    sc = CFG["simplify"]
    status, body = http(sc["url"])
    if status != 200:
        raise RuntimeError(f"Simplify HTTP {status}")
    out = []
    for r in json.loads(body):
        if not r.get("active") or not r.get("is_visible", True):
            continue
        if r.get("category") not in sc["categories"]:
            continue
        out.append({
            "source": "SimplifyJobs", "source_id": r["id"], "company": r["company_name"],
            "title": r["title"], "locations": r.get("locations", []), "url": r["url"],
            "posted": r.get("date_posted"), "sponsorship": r.get("sponsorship", ""),
        })
    return out


def fetch_greenhouse(token):  # public Job Board API
    s, b = http(f"https://boards-api.greenhouse.io/v1/boards/{token}/jobs")
    if s != 200:
        raise RuntimeError(f"Greenhouse {token} HTTP {s}")
    return [{"source": "Greenhouse", "source_id": str(j["id"]), "company": token, "title": j["title"],
             "locations": [j.get("location", {}).get("name", "")], "url": j["absolute_url"],
             "posted": int(datetime.fromisoformat(j["updated_at"]).timestamp()) if j.get("updated_at") else None,
             "sponsorship": ""} for j in json.loads(b).get("jobs", [])]


def fetch_lever(site):  # public Postings API
    s, b = http(f"https://api.lever.co/v0/postings/{site}?mode=json")
    if s != 200:
        raise RuntimeError(f"Lever {site} HTTP {s}")
    return [{"source": "Lever", "source_id": j["id"], "company": site, "title": j["text"],
             "locations": [j.get("categories", {}).get("location", "")], "url": j["hostedUrl"],
             "posted": int(j["createdAt"] / 1000) if j.get("createdAt") else None, "sponsorship": ""}
            for j in json.loads(b)]


def fetch_ashby(board):  # public Posting API
    s, b = http(f"https://api.ashbyhq.com/posting-api/job-board/{board}?includeCompensation=true")
    if s != 200:
        raise RuntimeError(f"Ashby {board} HTTP {s}")
    out = []
    for j in json.loads(b).get("jobs", []):
        if not j.get("isListed", True):
            continue
        pub = j.get("publishedAt")
        out.append({"source": "Ashby", "source_id": j["id"], "company": board, "title": j["title"],
                    "locations": [j.get("location", "")] + ([ "Remote"] if j.get("isRemote") else []),
                    "url": j.get("jobUrl") or j.get("applyUrl"),
                    "posted": int(datetime.fromisoformat(pub.replace("Z", "+00:00")).timestamp()) if pub else None,
                    "sponsorship": "",
                    "comp": (j.get("compensation") or {}).get("compensationTierSummary", "")})
    return out


# ---------- filtering ----------
def evaluate(job):
    """Returns (keep: bool, info: dict). Mandatory rules first; unknowns -> Needs review."""
    title = job["title"]
    if has_word(title, CFG["exclude_title_words"]):
        return False, {"reason": "level/role excluded"}
    if "citizenship" in job["sponsorship"].lower() or has_word(title, CFG["clearance_words"]):
        return False, {"reason": "citizenship/clearance required"}
    if re.search(r"\b[\w.]+ only\b", title.lower()):
        return False, {"reason": "restricted to one school"}
    rt = role_type(title)
    if not rt:
        return False, {"reason": "not a target role"}
    us = is_us(job["locations"])
    if us == "no":
        return False, {"reason": "non-US location"}
    elig, notes = "Needs review", []
    if us == "unknown":
        notes.append("Location not listed.")
    if any(c in norm(job["company"]) for c in map(norm, CFG.get("likely_restricted_companies", []))):
        notes.append("Defense/government contractor: many roles require US citizenship or clearance - check before applying.")
    notes.append("Description not yet checked for citizenship/clearance/sponsorship terms.")
    spons = job["sponsorship"]
    return True, {"role_type": rt, "eligibility": elig, "elig_notes": " ".join(notes),
                  "spons_flag": spons if spons and spons != "Other" else ""}


def triage_score(job, info):
    """Transparent 0-100 triage score from title, company, location, freshness.
    Not a hiring probability. Claude's description review can replace it later."""
    sc, parts = CFG["scoring"], []
    title = job["title"]
    pts = sc["role_points"].get(info["role_type"], 20)
    if has_word(title, sc["weak_role_words"]["words"]):
        pts = sc["weak_role_words"]["points"]
        parts.append(f"role {pts}/40 (weaker match: {', '.join(has_word(title, sc['weak_role_words']['words'])[:2])})")
    else:
        parts.append(f"role {pts}/40 ({info['role_type']})")
    total = pts
    if has_word(title, sc["level_two_words"]) and not has_word(title, ["1", "0", "i"]):
        p = 0; parts.append("level 0/20 (title says level 2/3)")
    elif has_word(title, sc["new_grad_words"]):
        p = sc["new_grad_points"]; parts.append(f"level {p}/20 (new-grad signal)")
    else:
        p = sc["no_signal_points"]; parts.append(f"level {p}/20 (no level in title)")
    total += p
    hits = has_word(title, sc["stack_words"])
    p = min(sc["stack_points"], 8 * len(hits)) if hits else 0
    total += p; parts.append(f"stack {p}/15" + (f" ({', '.join(hits[:3])})" if hits else ""))
    p = 0
    if job.get("posted"):
        age = (time.time() - job["posted"]) / 86400
        for days, val in sc["fresh_points"]:
            if age <= days:
                p = val; break
        parts.append(f"fresh {p}/15 ({age:.0f}d old)")
    else:
        parts.append("fresh 0/15 (no date)")
    total += p
    p = sc["us_points"] if is_us(job["locations"]) == "yes" else 5
    total += p; parts.append(f"location {p}/10")
    if "Defense/government contractor" in info.get("elig_notes", ""):
        total -= sc["restricted_penalty"]; parts.append(f"-{sc['restricted_penalty']} likely citizenship/clearance")
    total = max(0, min(100, total))
    label = "Strong" if total >= sc["strong"] else "Good" if total >= sc["good"] else "Low"
    return total, f"Triage {total} ({label}, title-based, v{sc['version']}): " + "; ".join(parts)


def notion_patch(page_id, props):
    tok, nc = os.environ["NOTION_TOKEN"], CFG["notion"]
    s, b = http(f"https://api.notion.com/v1/pages/{page_id}", "PATCH", {"properties": props},
                {"Authorization": f"Bearer {tok}", "Notion-Version": nc["api_version"], "Content-Type": "application/json"})
    if s != 200:
        raise RuntimeError(f"Notion PATCH {s}: {b[:200]!r}")
    time.sleep(0.35)


def notion_fit_notes(page_id):
    """Read a page's Fit Notes so code never overwrites Claude's 'Reviewed' scores."""
    tok, nc = os.environ["NOTION_TOKEN"], CFG["notion"]
    s, b = http(f"https://api.notion.com/v1/pages/{page_id}", "GET", None,
                {"Authorization": f"Bearer {tok}", "Notion-Version": nc["api_version"]})
    if s != 200:
        raise RuntimeError(f"Notion GET {s}: {b[:200]!r}")
    time.sleep(0.35)
    rt = json.loads(b)["properties"].get("Fit Notes", {}).get("rich_text", [])
    return "".join(x.get("plain_text", "") for x in rt)


def keys(job):
    primary = f'{job["source"]}:{job["source_id"]}'
    cross = f'{norm(job["company"])}|{norm(job["title"])}|{norm(" ".join(sorted(job["locations"])))}'
    return primary, cross


# ---------- outputs ----------
def notion_create(job, info, discovery, now_iso):
    tok = os.environ["NOTION_TOKEN"]
    nc = CFG["notion"]
    rt = lambda s: [{"text": {"content": (s or "")[:1900]}}]
    props = {
        "Title": {"title": rt(job["title"])},
        "Company": {"rich_text": rt(job["company"])},
        "Dedupe Key": {"rich_text": rt(keys(job)[1])},
        "Source Job ID": {"rich_text": rt(job["source_id"])},
        "Locations": {"rich_text": rt("; ".join(job["locations"]))},
        "Link": {"url": job["url"]},
        "Source": {"select": {"name": job["source"]}},
        "First Seen": {"date": {"start": now_iso}},
        "Discovery": {"select": {"name": discovery}},
        "Role Type": {"select": {"name": info["role_type"]}},
        "Eligibility": {"select": {"name": info["eligibility"]}},
        "Eligibility Notes": {"rich_text": rt(info["elig_notes"])},
        "Sponsorship Flag": {"rich_text": rt(info["spons_flag"])},
        "Compensation": {"rich_text": rt(job.get("comp", ""))},
        "Active": {"checkbox": True},
        "Last Checked Active": {"date": {"start": now_iso}},
        "Application Status": {"select": {"name": "Not started"}},
        "Outreach Status": {"select": {"name": "None"}},
        "Outcome": {"select": {"name": "Pending"}},
        "Fit Score": {"number": info["score"]},
        "Fit Notes": {"rich_text": rt(info["score_notes"])},
    }
    if job.get("posted"):
        props["Date Posted"] = {"date": {"start": datetime.fromtimestamp(job["posted"], timezone.utc).date().isoformat()}}
    s, b = http("https://api.notion.com/v1/pages", "POST",
                {"parent": {"database_id": nc["database_id"]}, "properties": props},
                {"Authorization": f"Bearer {tok}", "Notion-Version": nc["api_version"],
                 "Content-Type": "application/json"})
    if s != 200:
        raise RuntimeError(f"Notion HTTP {s}: {b[:300]!r}")
    time.sleep(0.35)  # stay under Notion's ~3 requests/second
    return json.loads(b)["id"]


def push(title, message, url=None, priority="default"):
    topic = os.environ.get("NTFY_TOPIC")
    if not topic or DRY:
        return
    h = {"Title": title.encode("ascii", "ignore").decode(), "Priority": priority, "Tags": "briefcase"}
    if url:
        h["Click"] = url
    http(f"https://ntfy.sh/{topic}", "POST", message.encode(), h)


def in_quiet_hours(now_local):
    q = CFG["quiet_hours"]
    return q["start"] <= now_local.hour < q["end"]


# ---------- main ----------
def main():
    t0 = time.time()
    now = datetime.now(timezone.utc)
    now_iso, now_local = now.isoformat(timespec="seconds"), now.astimezone(TZ)
    state = load_state()
    bootstrap = not state["bootstrapped"]
    run = {"time": now_iso, "bootstrap": bootstrap, "fetched": 0, "kept": 0, "created": 0,
           "dupes": 0, "skipped": {}, "errors": []}

    jobs = []
    fetchers = [("simplify", fetch_simplify)] if CFG["simplify"]["enabled"] else []
    for kind, fn in [("greenhouse", fetch_greenhouse), ("lever", fetch_lever), ("ashby", fetch_ashby)]:
        fetchers += [(f"{kind}:{n}", (lambda n=n, fn=fn: fn(n))) for n in CFG["boards"][kind]]
    for name, fn in fetchers:
        try:
            got = fn()
            jobs += got
            state["sources"][name] = {"last_success": now_iso, "count": len(got)}
        except Exception as e:
            run["errors"].append(f"{name}: {e}")
            state["sources"].setdefault(name, {})["last_error"] = f"{now_iso} {e}"
    run["fetched"] = len(jobs)

    seen = state["seen"]
    cross_seen = {v.get("cross") for v in seen.values()}
    created_now = []
    for job in sorted(jobs, key=lambda j: j.get("posted") or 0, reverse=True):
        primary, cross = keys(job)
        if primary in seen:
            continue
        if cross in cross_seen:  # same job from another source
            seen[primary] = {"cross": cross, "first_seen": now_iso, "dup": True}
            run["dupes"] += 1
            continue
        keep, info = evaluate(job)
        if not keep:
            seen[primary] = {"cross": cross, "first_seen": now_iso, "skip": info["reason"]}
            run["skipped"][info["reason"]] = run["skipped"].get(info["reason"], 0) + 1
            continue
        if bootstrap and job.get("posted") and time.time() - job["posted"] > CFG["bootstrap_max_age_days"] * 86400:
            seen[primary] = {"cross": cross, "first_seen": now_iso, "skip": "old at setup"}
            run["skipped"]["old at setup"] = run["skipped"].get("old at setup", 0) + 1
            continue
        info["score"], info["score_notes"] = triage_score(job, info)
        run["kept"] += 1
        if run["created"] >= CFG["max_new_rows_per_run"]:
            continue  # not marked seen -> picked up next run
        discovery = "Existing at setup" if bootstrap else "New"
        try:
            page_id = "dry-run" if DRY else notion_create(job, info, discovery, now_iso)
        except Exception as e:
            run["errors"].append(f"notion: {e}")
            break  # stop; unseen jobs retry next run (duplicate-safe)
        seen[primary] = {"cross": cross, "first_seen": now_iso, "page": page_id,
                         "score_v": CFG["scoring"]["version"], "score": info["score"]}
        cross_seen.add(cross)
        run["created"] += 1
        created_now.append((job, info))
        if run["created"] % 20 == 0:
            save_state(state)  # checkpoint

    # backfill: score older rows once (duplicate-safe; skips rows already scored)
    by_key = {keys(j)[0]: j for j in jobs}
    run["backfilled"] = 0
    for primary, v in list(seen.items()):
        if DRY or not v.get("page") or v.get("score_v") or run["backfilled"] >= 200:
            continue
        job = by_key.get(primary)
        if not job:
            continue  # job no longer listed; leave as is
        keep, info = evaluate(job)
        try:
            if notion_fit_notes(v["page"]).startswith("Reviewed"):
                v.update(score_v=CFG["scoring"]["version"], reviewed=True)
                continue
            if keep:
                score, notes = triage_score(job, info)
                notion_patch(v["page"], {"Fit Score": {"number": score},
                                         "Fit Notes": {"rich_text": [{"text": {"content": notes[:1900]}}]}})
                v.update(score_v=CFG["scoring"]["version"], score=score)
            else:
                notion_patch(v["page"], {"Eligibility": {"select": {"name": "Fail"}},
                                         "Application Status": {"select": {"name": "Skipped"}},
                                         "Fit Score": {"number": 0},
                                         "Fit Notes": {"rich_text": [{"text": {"content": "Skipped by updated filter: " + info["reason"]}}]}})
                v.update(score_v=CFG["scoring"]["version"], score=0, skip=info["reason"])
            run["backfilled"] += 1
        except Exception as e:
            run["errors"].append(f"backfill: {e}")
            break
        if run["backfilled"] % 25 == 0:
            save_state(state)

    # notifications: only for genuinely new jobs, never during quiet hours, never twice
    if not bootstrap:
        for job, info in created_now:
            item = {"t": f'{job["company"]} - {job["title"]}', "u": job["url"], "rt": info["role_type"]}
            if info["score"] >= CFG["scoring"]["strong"] and not in_quiet_hours(now_local):
                push(f'Strong match ({info["score"]})', f'{item["t"]}\n{"; ".join(job["locations"])[:120]}', job["url"], "high")
            else:
                state["pending_digest"].append(item)
        if state["pending_digest"] and not in_quiet_hours(now_local) and now_local.hour >= CFG["quiet_hours"]["end"]:
            d = state["pending_digest"]
            if len(d) >= 5 or now_local.hour in (7, 12, 18):
                lines = "\n".join(f'- {x["t"]}' for x in d[:15])
                push(f"{len(d)} new roles in Notion", lines + ("\n..." if len(d) > 15 else ""))
                state["pending_digest"] = []
    if run["errors"]:
        push("Job monitor problem", "\n".join(run["errors"])[:500], priority="high")

    state["bootstrapped"] = True if not run["errors"] or run["created"] else state["bootstrapped"]
    state["last_run"] = now_iso
    run["seconds"] = round(time.time() - t0, 1)
    save_state(state)
    if not DRY:
        with open(RUNLOG_PATH, "a") as f:
            f.write(json.dumps(run) + "\n")
    print(json.dumps(run, indent=1))
    if DRY:
        for job, info in created_now[:25]:
            print(f'{info["role_type"]:16} | {job["company"][:28]:28} | {job["title"][:60]:60} | {"; ".join(job["locations"])[:40]}')
    return 1 if run["errors"] and not run["created"] else 0


if __name__ == "__main__":
    sys.exit(main())
