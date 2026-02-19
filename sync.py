import os
import re
import time
import random
import requests
from datetime import datetime, timezone

# ====== ENV ======
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
NOTION_DB_ID = os.environ["NOTION_DB_ID"]
MISSEVAN_COOKIE = os.environ.get("MISSEVAN_COOKIE", "").strip()

# ====== Maoer / MissEvan APIs ======
GET_DRAMA = "https://www.missevan.com/dramaapi/getdrama"
GET_EPISODE_DETAILS = "https://www.missevan.com/dramaapi/getdramaepisodedetails"

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"

# ====== Update strategy ======
UPDATE_DAYS_SERIAL = 7  # only serial items update if last_sync >= 7 days

# ====== Retry strategy ======
MAX_RETRIES = 6
BASE_BACKOFF = 1.0  # seconds
JITTER = 0.3        # seconds


# =========================
# Notion helpers
# =========================
def notion_headers():
    return {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json",
    }


def notion_cover_payload(cover_url: str | None):
    if not cover_url:
        return None
    return {"type": "external", "external": {"url": cover_url}}


def _now_utc():
    return datetime.now(timezone.utc)


def _parse_iso_dt(s: str) -> datetime | None:
    """
    Notion date start usually like:
      2026-02-17T15:06:00.000Z
      2026-02-17T23:06:00+08:00
    """
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s)
    except Exception:
        return None


def notion_healthcheck():
    r = requests.get("https://api.notion.com/v1/users/me", headers=notion_headers(), timeout=30)
    print("NOTION /users/me:", r.status_code)
    if r.status_code != 200:
        print(r.text[:400])
    r.raise_for_status()


def notion_get_db_schema():
    """
    Read database properties so we only write fields that exist (avoid 400).
    return: dict[name] = type
    """
    r = requests.get(f"https://api.notion.com/v1/databases/{NOTION_DB_ID}", headers=notion_headers(), timeout=30)
    print("NOTION /databases/{id}:", r.status_code)
    if r.status_code != 200:
        print(r.text[:400])
    r.raise_for_status()

    j = r.json()
    props = j.get("properties", {}) if isinstance(j, dict) else {}
    return {k: (v.get("type") if isinstance(v, dict) else None) for k, v in props.items()}


def _get_prop_text(prop: dict) -> str:
    if not prop:
        return ""
    t = prop.get("type")

    if t == "rich_text":
        return "".join([x.get("plain_text", "") for x in prop.get("rich_text", [])])
    if t == "title":
        return "".join([x.get("plain_text", "") for x in prop.get("title", [])])
    if t == "url":
        return prop.get("url") or ""
    if t == "select":
        s = prop.get("select")
        return (s or {}).get("name", "") if s else ""
    if t == "number":
        v = prop.get("number")
        if v is None:
            return ""
        try:
            fv = float(v)
            return str(int(fv)) if fv.is_integer() else str(fv)
        except Exception:
            return str(v)
    if t == "date":
        d = prop.get("date") or {}
        return d.get("start") or ""
    if t == "checkbox":
        return "true" if prop.get("checkbox") else "false"

    return ""


def _get_prop_date_start(prop: dict) -> str:
    if not prop or prop.get("type") != "date":
        return ""
    d = prop.get("date") or {}
    return d.get("start") or ""


def _get_prop_checkbox(prop: dict) -> bool:
    if not prop or prop.get("type") != "checkbox":
        return False
    return bool(prop.get("checkbox"))


def notion_query_rows_target():
    """
    Target rows:
    - Work URL contains missevan.com/mdrama
    """
    url = f"https://api.notion.com/v1/databases/{NOTION_DB_ID}/query"
    body = {
        "page_size": 100,
        "filter": {"property": "Work URL", "url": {"contains": "missevan.com/mdrama"}},
    }

    rows = []
    next_cursor = None

    while True:
        if next_cursor:
            body["start_cursor"] = next_cursor

        r = requests.post(url, headers=notion_headers(), json=body, timeout=30)
        r.raise_for_status()
        data = r.json()

        for page in data.get("results", []):
            props = page.get("properties", {})
            page_id = page.get("id")

            work_url = _get_prop_text(props.get("Work URL")).strip()
            work_id_text = _get_prop_text(props.get("Work ID")).strip()
            platform = _get_prop_text(props.get("Platform")).strip()

            main_cv_override = _get_prop_text(props.get("Main CV Override")).strip()

            last_sync_start = _get_prop_date_start(props.get("Last Sync"))

            # IMPORTANT: use checkbox bool directly
            is_serial = _get_prop_checkbox(props.get("Is Serial"))

            rows.append(
                {
                    "page_id": page_id,
                    "work_url": work_url,
                    "work_id_text": work_id_text,
                    "platform": platform,
                    "main_cv_override": main_cv_override,
                    "last_sync_start": last_sync_start,
                    "is_serial_current": is_serial,
                }
            )

        if not data.get("has_more"):
            break
        next_cursor = data.get("next_cursor")

    return rows


def _request_with_retry(method: str, url: str, *, headers=None, json=None, params=None, timeout=30):
    """
    Retry for:
    - 502/503/504
    - 429 (respect Retry-After)
    Network errors also retry.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.request(method, url, headers=headers, json=json, params=params, timeout=timeout)

            if r.status_code == 429:
                ra = r.headers.get("Retry-After")
                wait = None
                if ra:
                    try:
                        wait = float(ra)
                    except Exception:
                        wait = None
                if wait is None:
                    wait = BASE_BACKOFF * (2 ** (attempt - 1))
                wait += random.random() * JITTER
                print(f"[retry] {method} {url} -> 429, sleep {wait:.2f}s (attempt {attempt}/{MAX_RETRIES})")
                time.sleep(wait)
                continue

            if r.status_code in (502, 503, 504):
                wait = BASE_BACKOFF * (2 ** (attempt - 1)) + random.random() * JITTER
                print(f"[retry] {method} {url} -> {r.status_code}, sleep {wait:.2f}s (attempt {attempt}/{MAX_RETRIES})")
                time.sleep(wait)
                continue

            return r

        except requests.RequestException as e:
            wait = BASE_BACKOFF * (2 ** (attempt - 1)) + random.random() * JITTER
            print(f"[retry] {method} {url} -> network error: {e}. sleep {wait:.2f}s (attempt {attempt}/{MAX_RETRIES})")
            time.sleep(wait)

    raise RuntimeError(f"Request failed after retries: {method} {url}")


def notion_update_page(page_id: str, properties: dict, cover_url: str | None = None):
    url = f"https://api.notion.com/v1/pages/{page_id}"
    body = {"properties": properties}
    cover = notion_cover_payload(cover_url)
    if cover:
        body["cover"] = cover

    r = _request_with_retry("PATCH", url, headers=notion_headers(), json=body, timeout=30)
    if r.status_code != 200:
        print("NOTION update failed:", r.status_code)
        print(r.text[:1000])
    r.raise_for_status()


# =========================
# Maoer helpers
# =========================
def maoer_headers(work_id: int):
    h = {
        "User-Agent": UA,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Referer": f"https://www.missevan.com/mdrama/{work_id}",
        "Origin": "https://www.missevan.com",
        "Connection": "keep-alive",
    }
    if MISSEVAN_COOKIE:
        h["Cookie"] = MISSEVAN_COOKIE
    return h


def maoer_get_drama(work_id: int) -> dict:
    r = requests.get(GET_DRAMA, params={"drama_id": work_id}, headers=maoer_headers(work_id), timeout=30)
    if r.status_code != 200:
        print("MAOER getdrama HTTP", r.status_code, r.text[:200])
    r.raise_for_status()

    j = r.json()
    info = j.get("info", {})
    drama = info.get("drama", info.get("Drama", {})) if isinstance(info, dict) else {}
    cvs = info.get("cvs", []) if isinstance(info, dict) else []
    return {"drama": drama or {}, "cvs": cvs or []}


def maoer_get_episode_details(work_id: int) -> dict:
    r = requests.get(
        GET_EPISODE_DETAILS,
        params={"drama_id": work_id, "p": 1, "page_size": 10},
        headers=maoer_headers(work_id),
        timeout=30,
    )
    if r.status_code != 200:
        print("MAOER episode_details HTTP", r.status_code)
        print("MAOER head:", r.text[:300])
    r.raise_for_status()

    j = r.json()
    info = j.get("info", {})
    return {"info": info or {}}


# =========================
# CV picking + Override
# =========================
BAD_WORDS_STRONG = [
    "导演", "监制", "制作", "策划", "编剧", "后期", "统筹", "录音", "配音导演",
    "旁白", "报幕", "字幕", "美工", "宣发", "运营", "音效", "混音", "母带",
]

MUSIC_WORDS_STRONG = [
    "演唱", "主题曲", "片尾曲", "插曲", "作词", "作曲", "编曲", "和声", "歌曲", "OST"
]


def pick_main_cvs(cvs: list, k: int = 4) -> str:
    candidates = []

    for item in cvs or []:
        character = (item.get("character") or "").strip()
        cv_info = item.get("cv_info") or {}
        name = (cv_info.get("name") or "").strip()
        group = (cv_info.get("group") or "").strip()

        if not name:
            continue
        if any(w in character for w in MUSIC_WORDS_STRONG):
            continue
        if any(w in character for w in BAD_WORDS_STRONG):
            continue

        score = 0
        if character:
            score += 20
            if "/" in character:
                score -= 4
            if len(character) > 10:
                score -= 3
        else:
            score -= 10

        candidates.append((score, character, name, group))

    if len(candidates) < k:
        for item in cvs or []:
            character = (item.get("character") or "").strip()
            cv_info = item.get("cv_info") or {}
            name = (cv_info.get("name") or "").strip()
            group = (cv_info.get("group") or "").strip()

            if not name:
                continue
            if any(w in character for w in MUSIC_WORDS_STRONG):
                continue
            if any(w in character for w in BAD_WORDS_STRONG):
                continue

            candidates.append((5, character, name, group))

    candidates.sort(key=lambda x: x[0], reverse=True)
    top = candidates[:k]

    if not top:
        return ""

    out = []
    for _, character, name, group in top:
        left = character if character else "角色未标注"
        out.append(f"{left} - {name}{f'({group})' if group else ''}")

    return "; ".join(out)


# =========================
# Fetch + parse
# =========================
def maoer_fetch(work_id: int) -> dict:
    meta = maoer_get_drama(work_id)
    detail = maoer_get_episode_details(work_id)

    drama = meta.get("drama", {})
    cvs = meta.get("cvs", [])
    info = detail.get("info", {})

    title = drama.get("name")
    cover_url = drama.get("cover")
    price = drama.get("price")
    is_serial = bool(drama.get("serialize"))
    newest_title = drama.get("newest")

    latest_count = None
    if isinstance(info, dict):
        pag = info.get("pagination", {})
        if isinstance(pag, dict) and pag.get("count") is not None:
            latest_count = pag.get("count")
        else:
            datas = info.get("Datas", [])
            if isinstance(datas, list) and datas:
                latest_count = len(datas)

    now_iso = _now_utc().isoformat()

    return {
        "title": title,
        "cover_url": cover_url,
        "price": price,
        "is_serial": is_serial,
        "newest_title": newest_title,
        "latest_count": latest_count,
        "cv_text": pick_main_cvs(cvs, k=4),
        "last_sync": now_iso,
    }


def parse_work_id(work_url: str, fallback: str):
    u = (work_url or "").strip()
    m = re.search(r"/mdrama/(?:drama/)?(\d+)", u)
    if m:
        return int(m.group(1))
    if fallback and fallback.isdigit():
        return int(fallback)
    return None


def should_update(is_serial_checked: bool, last_sync_start: str) -> bool:
    """
    New Strategy (your requirement):
    - Last Sync empty -> update immediately
    - Else: ONLY update when Is Serial is checked AND last_sync >= 7 days
    - If not serial (unchecked) -> never update (unless last sync is empty / unparseable)
    """
    if not last_sync_start:
        return True

    dt = _parse_iso_dt(last_sync_start)
    if not dt:
        # can't parse -> treat as needs update to fix the bad value
        return True

    if not is_serial_checked:
        return False

    age_days = (_now_utc() - dt.astimezone(timezone.utc)).total_seconds() / 86400.0
    return age_days >= UPDATE_DAYS_SERIAL


def build_props(schema: dict, work_id: int, work_url: str, data: dict):
    """
    Only write existing properties.
    Platform: always set "猫耳".
    """
    props = {}

    def put(name: str, payload: dict):
        if name in schema:
            props[name] = payload

    put("Title", {"title": [{"text": {"content": data.get("title") or f"猫耳-{work_id}"}}]})
    put("Platform", {"select": {"name": "猫耳"}})

    put("Work ID", {"rich_text": [{"text": {"content": str(work_id)}}]})
    put("Work URL", {"url": work_url or f"https://www.missevan.com/mdrama/{work_id}"})

    put("Cover URL", {"url": data.get("cover_url")})
    put("Price", {"number": data.get("price")})
    put("Is Serial", {"checkbox": bool(data.get("is_serial"))})
    put("Latest Episode", {"rich_text": [{"text": {"content": data.get("newest_title") or ""}}]})
    put("Latest Episode No", {"number": data.get("latest_count")})
    put("Last Sync", {"date": {"start": data.get("last_sync")}})

    if data.get("cv_text"):
        put("CV", {"rich_text": [{"text": {"content": data["cv_text"]}}]})

    return props


# =========================
# Main
# =========================
def main():
    notion_healthcheck()
    schema = notion_get_db_schema()

    rows = notion_query_rows_target()
    print("Notion target rows:", len(rows))

    for idx, row in enumerate(rows, start=1):
        page_id = row["page_id"]
        work_url = row["work_url"]
        work_id = parse_work_id(work_url, row["work_id_text"])

        if not work_id:
            print(f"[{idx}/{len(rows)}] SKIP (cannot parse Work ID). page:", page_id)
            print("  Work URL:", work_url)
            print("  Work ID:", row["work_id_text"])
            print("  Tip: 直接贴猫耳剧集详情页 URL（含 /mdrama/数字）")
            continue

        if not should_update(row.get("is_serial_current", False), row.get("last_sync_start", "")):
            print(f"[{idx}/{len(rows)}] skip (policy) {work_id} serial_checked={row.get('is_serial_current', False)}")
            continue

        data = maoer_fetch(work_id)

        # Manual override CV has highest priority
        override = (row.get("main_cv_override") or "").strip()
        if override:
            data["cv_text"] = override

        props = build_props(schema, work_id, work_url, data)

        notion_update_page(page_id, props, cover_url=data.get("cover_url"))

        print(
            f"[{idx}/{len(rows)}] updated {work_id} {data.get('title')} "
            f"count={data.get('latest_count')} serial_api={data.get('is_serial')}"
        )

        # small jitter to be polite
        time.sleep(0.6 + random.random() * 0.8)


if __name__ == "__main__":
    main()