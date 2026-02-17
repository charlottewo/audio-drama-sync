import os
import requests
from datetime import datetime, timezone
from typing import Dict, Any, List

NOTION_TOKEN = os.environ["NOTION_TOKEN"]
NOTION_DB_ID = os.environ["NOTION_DB_ID"]
MISSEVAN_COOKIE = os.environ.get("MISSEVAN_COOKIE", "").strip()

MAOER_DRAMA = "https://www.missevan.com/dramaapi/getdrama"
MAOER_EPISODE_DETAILS = "https://www.missevan.com/dramaapi/getdramaepisodedetails"

WORKS = [
    {
        "platform": "猫耳",
        "work_id": 91093,
        "work_url": "https://www.missevan.com/mdrama/91093",
    }
]


# ---------------- Notion ----------------
def notion_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json",
    }


def notion_healthcheck() -> None:
    r = requests.get("https://api.notion.com/v1/users/me", headers=notion_headers(), timeout=30)
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
    url = f"https://api.notion.com/v1/databases/{NOTION_DB_ID}/query"
    body = {"filter": {"property": "Key", "formula": {"string": {"equals": key}}}}
    r = requests.post(url, headers=notion_headers(), json=body, timeout=30)
    if r.status_code != 200:
        print("NOTION query failed:", r.status_code)
        print(r.text[:800])
    r.raise_for_status()
    return r.json().get("results", [])


def notion_update_page(page_id: str, properties: Dict[str, Any]) -> None:
    url = f"https://api.notion.com/v1/pages/{page_id}"
    r = requests.patch(url, headers=notion_headers(), json={"properties": properties}, timeout=30)
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


def build_properties(work: Dict[str, Any], data: Dict[str, Any], db_props: Dict[str, Any]) -> Dict[str, Any]:
    want: Dict[str, Any] = {}

    if "Title" in db_props:
        want["Title"] = {"title": [{"text": {"content": data.get("title") or f"猫耳-{work['work_id']}"}}]}

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
        want["Latest Episode No"] = {"number": data.get("total_count")}

    if "Last Sync" in db_props and data.get("last_sync"):
        want["Last Sync"] = {"date": {"start": data["last_sync"]}}

    return want


# ---------------- Missevan ----------------
def maoer_headers(work_id: int) -> Dict[str, str]:
    h = {
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
        h["Cookie"] = MISSEVAN_COOKIE
    return h


def fetch_drama_info(work_id: int) -> Dict[str, Any]:
    r = requests.get(MAOER_DRAMA, params={"drama_id": work_id}, headers=maoer_headers(work_id), timeout=30)
    if r.status_code != 200:
        print("MAOER getdrama HTTP", r.status_code, r.text[:300])
        r.raise_for_status()
    j = r.json()
    info = j.get("info", {}) or {}
    drama = info.get("drama", {}) or {}
    return {
        "title": drama.get("name"),
        "cover_url": drama.get("cover"),
        "price": drama.get("price"),
        "is_serial": bool(drama.get("serialize")),
        "newest_title": drama.get("newest"),
        "newest_episode_id": info.get("newest_episode_id"),
    }


def fetch_episode_list_info(work_id: int) -> Dict[str, Any]:
    # 这个接口有两种返回：info.episodes 或 info.Datas + pagination
    r = requests.get(
        MAOER_EPISODE_DETAILS,
        params={"drama_id": work_id, "p": 1, "page_size": 10},
        headers=maoer_headers(work_id),
        timeout=30,
    )
    if r.status_code != 200:
        print("MAOER getdramaepisodedetails HTTP", r.status_code, r.text[:300])
        r.raise_for_status()

    j = r.json()
    info = j.get("info", {}) or {}

    # 结构 1：你最早贴的那种（episodes）
    episodes_block = info.get("episodes")
    if isinstance(episodes_block, dict) and "episode" in episodes_block:
        ep_list = episodes_block.get("episode") or []
        # 这个列表不一定代表总数，所以尽量找 count 字段（如果有）
        total = None
        if isinstance(info.get("episodes"), dict):
            # 没有总数字段就先用最新页大小兜底（你要总数的话得翻页或找其他字段）
            total = len(ep_list) if isinstance(ep_list, list) else None
        newest_from_list = ep_list[-1].get("name") if isinstance(ep_list, list) and ep_list else None
        return {"newest_from_list": newest_from_list, "total_count": total}

    # 结构 2：你现在跑出来的（Datas + pagination）
    datas = info.get("Datas")
    pagination = info.get("pagination") or {}
    if isinstance(datas, list):
        newest_from_list = datas[0].get("soundstr") if datas else None
        total = pagination.get("count")  # 这里是总条数（你这次日志里 pagination 还没打，但结构就这样）
        return {"newest_from_list": newest_from_list, "total_count": total}

    return {"newest_from_list": None, "total_count": None}


def maoer_fetch(work_id: int) -> Dict[str, Any]:
    # 先拿剧集信息（title/cover/price/newest）
    drama = fetch_drama_info(work_id)

    # 再拿列表信息（总数/列表最新，作为兜底）
    epi = fetch_episode_list_info(work_id)

    # newest_title 优先用 getdrama 的 drama.newest；没有则用列表推断
    newest_title = drama.get("newest_title") or epi.get("newest_from_list")

    now_iso = datetime.now(timezone.utc).isoformat()

    return {
        "title": drama.get("title"),
        "cover_url": drama.get("cover_url"),
        "price": drama.get("price"),
        "is_serial": bool(drama.get("is_serial")),
        "newest_title": newest_title,
        "newest_episode_id": drama.get("newest_episode_id"),
        "total_count": epi.get("total_count"),
        "last_sync": now_iso,
    }


def main() -> None:
    notion_healthcheck()
    schema = notion_db_schema()
    db_props = schema.get("properties", {})

    for w in WORKS:
        if w.get("platform") != "猫耳":
            continue

        key = f"{w['platform']}:{w['work_id']}"
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
