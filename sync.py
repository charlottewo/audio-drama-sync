import os
import re
import requests
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

NOTION_TOKEN = os.environ["NOTION_TOKEN"]
NOTION_DB_ID = os.environ["NOTION_DB_ID"]
MISSEVAN_COOKIE = os.environ.get("MISSEVAN_COOKIE", "").strip()

MAOER_GET_DRAMA = "https://www.missevan.com/dramaapi/getdrama"
MAOER_EPISODE_DETAILS = "https://www.missevan.com/dramaapi/getdramaepisodedetails"

# 你的 Notion 字段名（必须完全一致）
PROP_TITLE = "Title"
PROP_PLATFORM = "Platform"
PROP_WORK_URL = "Work URL"
PROP_WORK_ID = "Work ID"
PROP_IS_SERIAL = "Is Serial"
PROP_LATEST_EP = "Latest Episode"
PROP_LATEST_NO = "Latest Episode No"
PROP_LAST_SYNC = "Last Sync"
PROP_KEY = "Key"  # 你的是 formula：Platform + ":" + Work ID


def notion_headers():
    return {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json",
    }


def extract_text_title(page: dict) -> str:
    t = page.get("properties", {}).get(PROP_TITLE, {}).get("title", [])
    if t and isinstance(t, list):
        return "".join([x.get("plain_text", "") for x in t])
    return ""


def get_prop_url(page: dict, prop_name: str) -> str:
    return page.get("properties", {}).get(prop_name, {}).get("url") or ""


def get_prop_select(page: dict, prop_name: str) -> str:
    s = page.get("properties", {}).get(prop_name, {}).get("select")
    return (s or {}).get("name") if isinstance(s, dict) else ""


def extract_drama_id(work_url: str) -> Optional[int]:
    if not work_url:
        return None
    m = re.search(r"/mdrama/(\d+)", work_url)
    if m:
        return int(m.group(1))
    m = re.search(r"[?&]drama_id=(\d+)", work_url)
    if m:
        return int(m.group(1))
    m = re.search(r"(\d{3,})", work_url)
    if m:
        return int(m.group(1))
    return None


def maoer_headers(referer_url: str):
    h = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Referer": referer_url,
        "Origin": "https://www.missevan.com",
        "Connection": "keep-alive",
    }
    if MISSEVAN_COOKIE:
        h["Cookie"] = MISSEVAN_COOKIE
    return h


def maoer_fetch_by_url(work_url: str) -> Dict[str, Any]:
    drama_id = extract_drama_id(work_url)
    if not drama_id:
        raise ValueError(f"无法从 URL 提取 drama_id：{work_url}")

    referer = f"https://www.missevan.com/mdrama/{drama_id}"
    headers = maoer_headers(referer)

    # ① getdrama：标题/封面/连载/最新一集标题
    r1 = requests.get(MAOER_GET_DRAMA, params={"drama_id": drama_id}, headers=headers, timeout=30)
    r1.raise_for_status()
    j1 = r1.json()
    info1 = j1.get("info", {})
    drama = info1.get("drama", {}) if isinstance(info1, dict) else {}

    title = drama.get("name")
    cover_url = drama.get("cover")
    is_serial = bool(drama.get("serialize"))
    newest_title = drama.get("newest")

    # ② episodedetails：拿总集数（优先 pagination.count）
    total_count = None
    r2 = requests.get(
        MAOER_EPISODE_DETAILS,
        params={"drama_id": drama_id, "p": 1, "page_size": 10},
        headers=headers,
        timeout=30
    )
    r2.raise_for_status()
    j2 = r2.json()
    info2 = j2.get("info", {})

    if isinstance(info2, dict):
        pag = info2.get("pagination")
        if isinstance(pag, dict) and isinstance(pag.get("count"), int):
            total_count = pag["count"]
        if total_count is None:
            eps = info2.get("episodes")
            if isinstance(eps, dict) and isinstance(eps.get("episode"), list):
                total_count = len(eps["episode"])

    now_iso = datetime.now(timezone.utc).isoformat()
    return {
        "platform": "猫耳",
        "work_id": drama_id,
        "work_url": referer,
        "title": title,
        "cover_url": cover_url,
        "is_serial": is_serial,
        "newest_title": newest_title,
        "latest_count": total_count,
        "last_sync": now_iso,
    }


def notion_db_query_all() -> List[dict]:
    url = f"https://api.notion.com/v1/databases/{NOTION_DB_ID}/query"
    results = []
    payload = {}
    while True:
        r = requests.post(url, headers=notion_headers(), json=payload, timeout=30)
        r.raise_for_status()
        j = r.json()
        results.extend(j.get("results", []))
        if j.get("has_more"):
            payload["start_cursor"] = j.get("next_cursor")
        else:
            break
    return results


def notion_update_page(page_id: str, properties: dict):
    url = f"https://api.notion.com/v1/pages/{page_id}"
    body = {"properties": properties}
    r = requests.patch(url, headers=notion_headers(), json=body, timeout=30)
    if r.status_code != 200:
        print("NOTION update failed:", r.status_code)
        print(r.text[:1200])
    r.raise_for_status()


def notion_props_from_data(data: dict) -> dict:
    # 你 Key 是 formula，所以这里不写 Key（让它自己算）
    return {
        PROP_TITLE: {"title": [{"text": {"content": data["title"] or f"猫耳-{data['work_id']}"}}]},
        PROP_PLATFORM: {"select": {"name": data["platform"]}},  # 默认猫耳
        PROP_WORK_ID: {"rich_text": [{"text": {"content": str(data["work_id"])}}]},
        PROP_WORK_URL: {"url": data.get("work_url")},
        "Cover URL": {"url": data.get("cover_url")},  # 你数据库里有这个字段
        PROP_IS_SERIAL: {"checkbox": bool(data.get("is_serial"))},
        PROP_LATEST_EP: {"rich_text": [{"text": {"content": data.get("newest_title") or ""}}]},
        PROP_LATEST_NO: {"number": data.get("latest_count") if isinstance(data.get("latest_count"), int) else None},
        PROP_LAST_SYNC: {"date": {"start": data.get("last_sync")}},
    }


def main():
    pages = notion_db_query_all()
    print("NOTION pages:", len(pages))

    for p in pages:
        page_id = p["id"]
        work_url = get_prop_url(p, PROP_WORK_URL).strip()
        if not work_url:
            continue

        # 平台默认猫耳；如果你将来加别的平台，可以从 Notion select 读出来
        platform = get_prop_select(p, PROP_PLATFORM).strip() or "猫耳"
        if platform != "猫耳":
            continue

        try:
            data = maoer_fetch_by_url(work_url)
            props = notion_props_from_data(data)
            notion_update_page(page_id, props)
            print("updated", data["work_id"], data.get("title"), data.get("newest_title"))
        except Exception as e:
            print("FAILED:", work_url, "=>", repr(e))


if __name__ == "__main__":
    main()
