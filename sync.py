import os
import re
import requests
from datetime import datetime, timezone
from typing import Any

# ====== ENV ======
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
NOTION_DB_ID = os.environ["NOTION_DB_ID"]
MISSEVAN_COOKIE = os.environ.get("MISSEVAN_COOKIE", "")

# ====== MissEvan APIs ======
GET_DRAMA = "https://www.missevan.com/dramaapi/getdrama"
GET_EPISODE_DETAILS = "https://www.missevan.com/dramaapi/getdramaepisodedetails"

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

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

def notion_healthcheck():
    r = requests.get("https://api.notion.com/v1/users/me", headers=notion_headers(), timeout=30)
    print("NOTION /users/me:", r.status_code)
    if r.status_code != 200:
        print(r.text[:400])
    r.raise_for_status()

def notion_db_check():
    r = requests.get(f"https://api.notion.com/v1/databases/{NOTION_DB_ID}", headers=notion_headers(), timeout=30)
    print("NOTION /databases/{id}:", r.status_code)
    if r.status_code != 200:
        print(r.text[:400])
    r.raise_for_status()

def _get_prop_text(prop: dict) -> str:
    """Read rich_text/title/url/select plain text from a Notion property."""
    if not prop:
        return ""
    t = prop.get("type")
    if t == "rich_text":
        return "".join([x.get("plain_text", "") for x in prop.get("rich_text", [])]).strip()
    if t == "title":
        return "".join([x.get("plain_text", "") for x in prop.get("title", [])]).strip()
    if t == "url":
        return (prop.get("url") or "").strip()
    if t == "select":
        s = prop.get("select")
        return ((s or {}).get("name", "") if s else "").strip()
    if t == "number":
        v = prop.get("number")
        return "" if v is None else str(v)
    if t == "formula":
        # formula 读值时会在 prop["formula"] 里
        f = prop.get("formula") or {}
        # 可能是 string/number/boolean/date
        for k in ("string", "number", "boolean"):
            if k in f and f[k] is not None:
                return str(f[k]).strip()
        if "date" in f and f["date"]:
            # date 对我们没用，返回空
            return ""
    return ""

def notion_list_maoer_rows():
    """
    拉取 Platform=猫耳 的所有行。
    新增剧集：只要新建一行，Platform 选猫耳，填 Work URL 或 Work ID。
    """
    url = f"https://api.notion.com/v1/databases/{NOTION_DB_ID}/query"
    body = {
        "page_size": 100,
        "filter": {"property": "Platform", "select": {"equals": "猫耳"}},
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

            work_url = _get_prop_text(props.get("Work URL"))
            work_id_text = _get_prop_text(props.get("Work ID"))
            main_cv_override = _get_prop_text(props.get("Main CV Override"))

            rows.append(
                {
                    "page_id": page_id,
                    "work_url": work_url,
                    "work_id_text": work_id_text,
                    "main_cv_override": main_cv_override,
                }
            )

        if not data.get("has_more"):
            break
        next_cursor = data.get("next_cursor")

    return rows

def notion_update_page(page_id: str, properties: dict, cover_url: str | None = None):
    url = f"https://api.notion.com/v1/pages/{page_id}"

    body: dict[str, Any] = {"properties": properties}
    cover = notion_cover_payload(cover_url)
    if cover:
        body["cover"] = cover

    r = requests.patch(url, headers=notion_headers(), json=body, timeout=30)
    if r.status_code != 200:
        print("NOTION update failed:", r.status_code)
        print(r.text[:800])
    r.raise_for_status()

# =========================
# MissEvan helpers
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
    return {"drama": drama or {}}

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

def _extract_cv_names_from_drama(drama: dict) -> list[str]:
    """
    尽量从 getdrama 的 drama 里抽 CV 名单（字段不稳定，做容错）。
    我们只要“展示用”，所以拿到能用的就行。
    """
    if not isinstance(drama, dict):
        return []

    candidates: list[str] = []

    # 常见：cvs: [{name:...}, ...]
    cvs = drama.get("cvs") or drama.get("Cvs") or drama.get("CVS")
    if isinstance(cvs, list):
        for item in cvs:
            if isinstance(item, dict):
                n = item.get("name") or item.get("nickname") or item.get("uname")
                if n:
                    candidates.append(str(n).strip())
            elif isinstance(item, str):
                candidates.append(item.strip())

    # 有些会直接给字符串
    cv_str = drama.get("cv") or drama.get("CV")
    if isinstance(cv_str, str) and cv_str.strip():
        # 可能是 "A/B/C" 之类
        parts = re.split(r"[、/，,·\s]+", cv_str.strip())
        candidates.extend([p for p in (x.strip() for x in parts) if p])

    # 去重保序
    seen = set()
    out = []
    for n in candidates:
        if n and n not in seen:
            seen.add(n)
            out.append(n)
    return out

# =========================
# Fetch + parse
# =========================
def maoer_fetch(work_id: int) -> dict:
    meta = maoer_get_drama(work_id)               # 标题/封面/价格/连载/（可能含CV）
    detail = maoer_get_episode_details(work_id)   # pagination.count 作为总集数

    drama = meta.get("drama", {})
    info = detail.get("info", {})

    title = drama.get("name")
    cover_url = drama.get("cover")
    is_serial = bool(drama.get("serialize"))
    price = drama.get("price")  # 可能是 int/float/None

    # “更新到第几集”：用 pagination.count（总条数）
    latest_count = None
    if isinstance(info, dict):
        pag = info.get("pagination", {})
        if isinstance(pag, dict) and pag.get("count") is not None:
            latest_count = pag.get("count")

    cv_names = _extract_cv_names_from_drama(drama)

    now_iso = datetime.now(timezone.utc).isoformat()
    return {
        "title": title,
        "cover_url": cover_url,
        "is_serial": is_serial,
        "latest_count": latest_count,
        "price": price,
        "cv_names": cv_names,  # list[str]
        "last_sync": now_iso,
    }

def parse_work_id(work_url: str, fallback: str):
    """
    兼容：
    - https://www.missevan.com/mdrama/91093
    - https://www.missevan.com/mdrama/drama/26870
    """
    u = (work_url or "").strip()
    m = re.search(r"/mdrama/(?:drama/)?(\d+)", u)
    if m:
        return int(m.group(1))

    if fallback and fallback.isdigit():
        return int(fallback)

    return None

def _rt(text: str):
    return {"rich_text": [{"text": {"content": text}}]}

def notion_properties_for_work(work_id: int, work_url: str, data: dict, main_cv_override: str):
    """
    重点：
    - CV：优先 Main CV Override；否则取抓到的前4位；如果抓不到就【不更新CV】（避免清空）。
    """
    props: dict[str, Any] = {
        "Title": {"title": [{"text": {"content": data.get("title") or f"猫耳-{work_id}"}}]},
        "Platform": {"select": {"name": "猫耳"}},
        "Work ID": _rt(str(work_id)),
        "Work URL": {"url": work_url or f"https://www.missevan.com/mdrama/{work_id}"},
        "Is Serial": {"checkbox": bool(data.get("is_serial"))},
        "Latest Episode No": {"number": data.get("latest_count")},
        "Last Sync": {"date": {"start": data.get("last_sync")}},
        "Cover URL": {"url": data.get("cover_url")},
    }

    # Price：只有拿得到才写（拿不到就不动，避免写 None 导致你字段被清）
    if data.get("price") is not None:
        props["Price"] = {"number": data.get("price")}

    # CV：override > fetched top4 > 不更新（保留原有）
    override = (main_cv_override or "").strip()
    if override:
        props["CV"] = _rt(override)
        cv_debug = "(override)"
    else:
        cv_names = data.get("cv_names") or []
        cv_top = [x for x in cv_names if x][:4]
        if cv_top:
            props["CV"] = _rt(" / ".join(cv_top))
            cv_debug = " / ".join(cv_top)
        else:
            # 关键：不写 CV，就不会清空 Notion 原值
            cv_debug = ""

    return props, cv_debug

# =========================
# Main
# =========================
def main():
    notion_healthcheck()
    notion_db_check()

    rows = notion_list_maoer_rows()
    print("Notion maoer rows:", len(rows))

    for row in rows:
        page_id = row["page_id"]
        work_url = row["work_url"]
        work_id = parse_work_id(work_url, row["work_id_text"])

        if not work_id:
            print("SKIP (cannot parse Work ID). page:", page_id)
            print("  Work URL:", work_url)
            print("  Work ID:", row["work_id_text"])
            print("  Tip: Platform 选猫耳，并确保 Work URL 含 /mdrama/数字")
            continue

        data = maoer_fetch(work_id)
        props, cv_debug = notion_properties_for_work(
            work_id=work_id,
            work_url=work_url,
            data=data,
            main_cv_override=row.get("main_cv_override", ""),
        )

        notion_update_page(page_id, props, cover_url=data.get("cover_url"))

        print(
            "updated",
            work_id,
            data.get("title"),
            f"count={data.get('latest_count')}",
            f"serial={data.get('is_serial')}",
            f"price={data.get('price')}",
            f"cv={cv_debug}",
        )

if __name__ == "__main__":
    main()
