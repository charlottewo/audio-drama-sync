import os
import requests
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

# --- ENV ---
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
NOTION_DB_ID = os.environ["NOTION_DB_ID"]
MISSEVAN_COOKIE = os.environ.get("MISSEVAN_COOKIE", "").strip()

# --- CONST ---
MAOER_EPISODE_DETAILS = "https://www.missevan.com/dramaapi/getdramaepisodedetails"

WORKS = [
    {
        "platform": "猫耳",
        "work_id": 91093,
        "work_url": "https://www.missevan.com/mdrama/91093",
    }
]


# ---------------- Notion helpers ----------------
def notion_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json",
    }


def notion_healthcheck() -> None:
    url = "https://api.notion.com/v1/users/me"
    r = requests.get(url, headers=notion_headers(), timeout=30)
    print("NOTION /users/me:", r.status_code)
    if r.status_code != 200:
        print(r.text[:800])
        r.raise_for_status()


def notion_db_schema() -> Dict[str, Any]:
    url = f"https://api.notion.com/v1/databases/{NOTION_DB_ID}"
    r = requests.get(url, headers=notion_headers(), timeout=30)
    print("NOTION /databases/{id}:", r.status_code)
    if r.status_code != 200:
        print(r.text[:800])
        r.raise_for_status()
    return r.json()


def notion_query_by_key(key: str) -> List[Dict[str, Any]]:
    # Key 是 formula：format(prop("Platform")) + ":" + prop("Work ID")
    url = f"https://api.notion.com/v1/databases/{NOTION_DB_ID}/query"
    body = {
        "filter": {
            "property": "Key",
            "formula": {"string": {"equals": key}},
        }
    }
    r = requests.post(url, headers=notion_headers(), json=body, timeout=30)
    if r.status_code != 200:
        print("NOTION query failed:", r.status_code)
        print(r.text[:800])
    r.raise_for_status()
    return r.json().get("results", [])


def notion_update_page(page_id: str, properties: Dict[str, Any]) -> None:
    url = f"https://api.notion.com/v1/pages/{page_id}"
    body = {"properties": properties}
    r = requests.patch(url, headers=notion_headers(), json=body, timeout=30)
    if r.status_code != 200:
        print("NOTION update failed:", r.status_code)
        print(r.text[:800])
    r.raise_for_status()


def notion_create_page(properties: Dict[str, Any]) -> None:
    url = "https://api.notion.com/v1/pages"
    body = {"parent": {"database_id": NOTION_DB_ID}, "properties": properties}
    r = requests.post(url, headers=notion_headers(), json=body, timeout=30)
    if r.status_code != 200:
        print("NOTION create failed:", r.status_code)
        print(r.text[:800])
    r.raise_for_status()


# ---------------- Missevan helpers ----------------
def maoer_fetch(work_id: int) -> Dict[str, Any]:
    params = {"drama_id": work_id, "p": 1, "page_size": 10}

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Referer": f"https://www.missevan.com/mdrama/{work_id}",
        "Origin": "https://www.missevan.com",
        "Connection": "keep-alive",
    }
    if MISSEVAN_COOKIE:
        headers["Cookie"] = MISSEVAN_COOKIE

    r = requests.get(MAOER_EPISODE_DETAILS, params=params, headers=headers, timeout=30)
    print("MAOER status:", r.status_code)
print("MAOER content-type:", r.headers.get("content-type"))
print("MAOER head:", r.text[:200])

try:
    j = r.json()
    print("MAOER json keys:", list(j.keys())[:20])
    print("MAOER code/msg:", j.get("code"), j.get("msg"))
except Exception as e:
    print("MAOER json parse failed:", e)
    raise

    if r.status_code != 200:
        print("MISSEVAN HTTP", r.status_code)
        print("Response head:", r.text[:300])
        r.raise_for_status()

    j = r.json()
    info = j.get("info", {})
    drama = info.get("drama", {})

    title = drama.get("name")
    cover_url = drama.get("cover")
    price = drama.get("price")
    is_serial = bool(drama.get("serialize"))
    newest_title = drama.get("newest")
    newest_episode_id = info.get("newest_episode_id")

    episodes_block = info.get("episodes", {})
    episode_list = episodes_block.get("episode", []) if isinstance(episodes_block, dict) else []
    latest_count = len(episode_list) if episode_list else None

    now_iso = datetime.now(timezone.utc).isoformat()

    return {
        "title": title,
        "cover_url": cover_url,
        "price": price,
        "is_serial": is_serial,
        "newest_title": newest_title,
        "newest_episode_id": newest_episode_id,
        "latest_count": latest_count,
        "last_sync": now_iso,
    }


# ---------------- Property builder ----------------
def build_properties(work: Dict[str, Any], data: Dict[str, Any], db_props: Dict[str, Any]) -> Dict[str, Any]:
    """
    只写数据库里确实存在的属性，避免你手搓字段少了就 400。
    Key 是 formula，不写入。
    """
    want: Dict[str, Any] = {}

    # Title (Notion 的标题属性名必须跟你库里一致：你这里就叫 Title)
    if "Title" in db_props:
        want["Title"] = {"title": [{"text": {"content": data["title"] or f"猫耳-{work['work_id']}"}}]}

    if "Platform" in db_props:
        want["Platform"] = {"select": {"name": work["platform"]}}

    if "Work ID" in db_props:
        want["Work ID"] = {"rich_text": [{"text": {"content": str(work["work_id"])}}]}

    if "Work URL" in db_props:
        want["Work URL"] = {"url": work.get("work_url")}

    if "Cover URL" in db_props:
        want["Cover URL"] = {"url": data.get("cover_url")}

    if "Price" in db_props:
        want["Price"] = {"number": data.get("price")}

    if "Is Serial" in db_props:
        want["Is Serial"] = {"checkbox": bool(data.get("is_serial"))}

    if "Latest Episode" in db_props:
        want["Latest Episode"] = {"rich_text": [{"text": {"content": data.get("newest_title") or ""}}]}

    if "Latest Episode No" in db_props:
        want["Latest Episode No"] = {"number": data.get("latest_count")}

    # 你库里现在没有这个字段就不写；你想要的话自己在 Notion 里加一个 Date 字段叫 Last Sync
    if "Last Sync" in db_props:
        want["Last Sync"] = {"date": {"start": data.get("last_sync")}}

    return want


def main() -> None:
    notion_healthcheck()
    schema = notion_db_schema()
    db_props = schema.get("properties", {})

    for w in WORKS:
        if w.get("platform") != "猫耳":
            continue

        key = f"{w['platform']}:{w['work_id']}"  # 用于查 Key formula
        data = maoer_fetch(w["work_id"])
        props = build_properties(w, data, db_props)

        existing = notion_query_by_key(key)
        if existing:
            page_id = existing[0]["id"]
            notion_update_page(page_id, props)
            print("updated", w["work_id"], data.get("title"), data.get("newest_title"))
        else:
            notion_create_page(props)
            print("created", w["work_id"], data.get("title"), data.get("newest_title"))


if __name__ == "__main__":
    main()
