import os
import re
import requests
from datetime import datetime, timezone

# ====== ENV ======
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
NOTION_DB_ID = os.environ["NOTION_DB_ID"]
MISSEVAN_COOKIE = os.environ.get("MISSEVAN_COOKIE", "")

# ====== Maoer / MissEvan APIs ======
GET_DRAMA = "https://www.missevan.com/dramaapi/getdrama"
GET_EPISODE_DETAILS = "https://www.missevan.com/dramaapi/getdramaepisodedetails"

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"

# ====== Notion property names (按你数据库列名来) ======
PROP_TITLE = "Title"
PROP_PLATFORM = "Platform"
PROP_WORK_ID = "Work ID"
PROP_WORK_URL = "Work URL"
PROP_COVER_URL = "Cover URL"
PROP_IS_SERIAL = "Is Serial"
PROP_LATEST_NO = "Latest Episode No"
PROP_LAST_SYNC = "Last Sync"
PROP_PRICE = "Price"
PROP_CV = "CV"
PROP_CV_OVERRIDE = "Main CV Override"   # 你手动填主役，用来 override 自动抓取


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
        return "" if v is None else str(v)
    return ""


def notion_list_maoer_rows():
    """
    拉取 Platform=猫耳 的所有行。
    新增剧集：新建一行，Platform选猫耳，填 Work URL 或 Work ID。
    CV override：Main CV Override 你手填就生效。
    """
    url = f"https://api.notion.com/v1/databases/{NOTION_DB_ID}/query"
    body = {
        "page_size": 100,
        "filter": {"property": PROP_PLATFORM, "select": {"equals": "猫耳"}},
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
            rows.append(
                {
                    "page_id": page.get("id"),
                    "work_url": _get_prop_text(props.get(PROP_WORK_URL)).strip(),
                    "work_id_text": _get_prop_text(props.get(PROP_WORK_ID)).strip(),
                    "cv_override": _get_prop_text(props.get(PROP_CV_OVERRIDE)).strip(),
                }
            )

        if not data.get("has_more"):
            break
        next_cursor = data.get("next_cursor")

    return rows


def notion_update_page(page_id: str, properties: dict, cover_url: str | None = None):
    url = f"https://api.notion.com/v1/pages/{page_id}"
    body = {"properties": properties}

    cover = notion_cover_payload(cover_url)
    if cover:
        body["cover"] = cover

    r = requests.patch(url, headers=notion_headers(), json=body, timeout=30)
    if r.status_code != 200:
        print("NOTION update failed:", r.status_code)
        print(r.text[:800])
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
    return {"info": j.get("info", {}) or {}}


# =========================
# Parsing
# =========================
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


def clean_cv_list(cvs: list, limit: int = 4) -> list[str]:
    """
    cvs 元素形如：
    {"character": "...", "cv_info": {"name": "..."}}
    过滤掉“演唱/主题曲/片尾曲/歌曲”等更像歌手信息的条目。
    """
    bad_kw = ("演唱", "主题曲", "片尾曲", "歌曲", "插曲", "曲", "歌")
    names = []
    for item in cvs or []:
        character = (item.get("character") or "").strip()
        cv_info = item.get("cv_info") or {}
        name = (cv_info.get("name") or "").strip()
        if not name:
            continue
        if any(k in character for k in bad_kw):
            continue
        # 去重（保持顺序）
        if name not in names:
            names.append(name)
        if len(names) >= limit:
            break
    return names


def maoer_fetch(work_id: int) -> dict:
    meta = maoer_get_drama(work_id)               # title/cover/price/serialize
    detail = maoer_get_episode_details(work_id)   # pagination + cvs

    drama = meta.get("drama", {})
    info = detail.get("info", {})

    title = drama.get("name")
    cover_url = drama.get("cover")
    is_serial = bool(drama.get("serialize"))

    price = drama.get("price")  # 这里就是 199 这种（钻石/标价），你 Notion 里按 number 存

    latest_count = None
    if isinstance(info, dict):
        pag = info.get("pagination", {})
        if isinstance(pag, dict) and pag.get("count") is not None:
            latest_count = pag.get("count")

    cvs = []
    if isinstance(info, dict):
        cvs = info.get("cvs", []) or []

    cv_names = clean_cv_list(cvs, limit=4)
    cv_text = " / ".join(cv_names) if cv_names else ""

    now_iso = datetime.now(timezone.utc).isoformat()

    return {
        "title": title,
        "cover_url": cover_url,
        "is_serial": is_serial,
        "latest_count": latest_count,
        "price": price,
        "cv_text": cv_text,
        "last_sync": now_iso,
    }


def notion_properties_for_work(work_id: int, work_url: str, data: dict, cv_override: str = ""):
    """
    写回 Notion：
    - Title
    - Latest Episode No
    - Is Serial
    - Price
    - CV（支持 override）
    """
    cv_final = (cv_override or "").strip() or (data.get("cv_text") or "")

    props = {
        PROP_TITLE: {"title": [{"text": {"content": data.get("title") or f"猫耳-{work_id}"}}]},
        PROP_PLATFORM: {"select": {"name": "猫耳"}},
        PROP_WORK_ID: {"rich_text": [{"text": {"content": str(work_id)}}]},
        PROP_WORK_URL: {"url": work_url or f"https://www.missevan.com/mdrama/{work_id}"},
        PROP_IS_SERIAL: {"checkbox": bool(data.get("is_serial"))},
        PROP_LATEST_NO: {"number": data.get("latest_count")},
        PROP_LAST_SYNC: {"date": {"start": data.get("last_sync")}},
        PROP_COVER_URL: {"url": data.get("cover_url")},
        PROP_PRICE: {"number": data.get("price") if isinstance(data.get("price"), (int, float)) else None},
        PROP_CV: {"rich_text": [{"text": {"content": cv_final}}]},
    }
    return props


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
            continue

        data = maoer_fetch(work_id)
        props = notion_properties_for_work(work_id, work_url, data, cv_override=row.get("cv_override", ""))

        notion_update_page(page_id, props, cover_url=data.get("cover_url"))

        print(
            "updated",
            work_id,
            data.get("title"),
            f"count={data.get('latest_count')}",
            f"serial={data.get('is_serial')}",
            f"price={data.get('price')}",
            f"cv={data.get('cv_text')[:40]}",
            "(override)" if (row.get("cv_override") or "").strip() else "",
        )


if __name__ == "__main__":
    main()
