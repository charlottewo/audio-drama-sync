import os
import requests
from datetime import datetime, timezone

NOTION_TOKEN = os.environ["NOTION_TOKEN"]
NOTION_DB_ID = os.environ["NOTION_DB_ID"]
MISSEVAN_COOKIE = os.environ.get("MISSEVAN_COOKIE", "")

MAOER_EPISODE_DETAILS = "https://www.missevan.com/dramaapi/getdramaepisodedetails"

WORKS = [
    {
        "platform": "猫耳",
        "work_id": 91093,
        "work_url": "https://www.missevan.com/mdrama/91093",
    }
]


def notion_headers():
    return {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json",
    }


def notion_query_by_key(key: str):
    url = f"https://api.notion.com/v1/databases/{NOTION_DB_ID}/query"
    body = {
        "filter": {
            "property": "Key",
            "formula": {"string": {"equals": key}},
        }
    }
    r = requests.post(url, headers=notion_headers(), json=body, timeout=30)
    r.raise_for_status()
    return r.json().get("results", [])


def notion_update_page(page_id: str, properties: dict):
    url = f"https://api.notion.com/v1/pages/{page_id}"
    body = {"properties": properties}
    r = requests.patch(url, headers=notion_headers(), json=body, timeout=30)
    if r.status_code != 200:
        print("NOTION update failed:", r.status_code)
        print(r.text[:800])
    r.raise_for_status()


def notion_create_page(properties: dict):
    url = "https://api.notion.com/v1/pages"
    body = {"parent": {"database_id": NOTION_DB_ID}, "properties": properties}
    r = requests.post(url, headers=notion_headers(), json=body, timeout=30)
    r.raise_for_status()


def maoer_fetch(work_id: int):
    params = {"drama_id": work_id, "p": 1, "page_size": 10}

    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Referer": f"https://www.missevan.com/mdrama/{work_id}",
        "Origin": "https://www.missevan.com",
        "Connection": "keep-alive",
    }

    if MISSEVAN_COOKIE:
        headers["Cookie"] = MISSEVAN_COOKIE

    r = requests.get(MAOER_EPISODE_DETAILS, params=params, headers=headers, timeout=30)

    if r.status_code != 200:
        print("HTTP", r.status_code)
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


def notion_properties_for_work(work: dict, data: dict):
    props = {
        "Title": {"title": [{"text": {"content": data["title"] or f"猫耳-{work['work_id']}"}}]},
        "Platform": {"select": {"name": work["platform"]}},
        "Work ID": {"rich_text": [{"text": {"content": str(work["work_id"])}}]},
        "Work URL": {"url": work.get("work_url")},
        "Cover URL": {"url": data.get("cover_url")},
        "Price": {"number": data.get("price")},
        "Is Serial": {"checkbox": bool(data.get("is_serial"))},
        "Latest Episode": {"rich_text": [{"text": {"content": data.get("newest_title") or ""}}]},
        "Latest Episode No": {"number": data.get("latest_count")},
        "Last Sync": {"date": {"start": data.get("last_sync")}},
    }
    return props

def notion_healthcheck():
    url = "https://api.notion.com/v1/users/me"
    r = requests.get(url, headers=notion_headers(), timeout=30)
    print("NOTION /users/me:", r.status_code)
    if r.status_code != 200:
        print(r.text[:300])
        r.raise_for_status()

def notion_db_check():
    url = f"https://api.notion.com/v1/databases/{NOTION_DB_ID}"
    r = requests.get(url, headers=notion_headers(), timeout=30)
    print("NOTION /databases/{id}:", r.status_code)
    if r.status_code != 200:
        print(r.text[:300])
        r.raise_for_status()

def main():
    notion_healthcheck()
    notion_db_check()
    for w in WORKS:
        if w["platform"] != "猫耳":
            continue

        data = maoer_fetch(w["work_id"])
        key = f"{w['platform']}:{w['work_id']}"

        props = notion_properties_for_work(w, data)
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
