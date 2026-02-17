import os
import json
import re
import requests
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List


NOTION_TOKEN = os.environ["NOTION_TOKEN"]
NOTION_DB_ID = os.environ["NOTION_DB_ID"]
MISSEVAN_COOKIE = os.environ.get("MISSEVAN_COOKIE", "").strip()

MAOER_GET_DRAMA = "https://www.missevan.com/dramaapi/getdrama"
MAOER_EPISODE_DETAILS = "https://www.missevan.com/dramaapi/getdramaepisodedetails"


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
        print(r.text[:1000])
    r.raise_for_status()


def notion_create_page(properties: dict):
    url = "https://api.notion.com/v1/pages"
    body = {"parent": {"database_id": NOTION_DB_ID}, "properties": properties}
    r = requests.post(url, headers=notion_headers(), json=body, timeout=30)
    if r.status_code != 200:
        print("NOTION create failed:", r.status_code)
        print(r.text[:1000])
    r.raise_for_status()


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


def extract_drama_id(work_url: str) -> Optional[int]:
    """
    支持：
    - https://www.missevan.com/mdrama/91093
    - https://www.missevan.com/dramaapi/getdrama?drama_id=91093
    - 任何包含 /mdrama/<id> 的 URL
    """
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


def load_works_from_json(path: str = "works.json") -> Dict[str, Any]:
    """
    works.json:
    {
      "platform": "猫耳",
      "works": ["https://www.missevan.com/mdrama/91093"]
    }
    """
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    platform = data.get("platform") or "猫耳"
    urls = data.get("works") or []
    urls = [u.strip() for u in urls if isinstance(u, str) and u.strip()]
    return {"platform": platform, "urls": urls}


def maoer_fetch(work_url: str) -> Dict[str, Any]:
    drama_id = extract_drama_id(work_url)
    if not drama_id:
        raise ValueError(f"无法从 URL 提取 drama_id：{work_url}")

    referer = f"https://www.missevan.com/mdrama/{drama_id}"
    headers = maoer_headers(referer)

    # ① 先拿剧集基本信息（标题/封面/连载/最新一集标题等）
    r1 = requests.get(MAOER_GET_DRAMA, params={"drama_id": drama_id}, headers=headers, timeout=30)
    if r1.status_code != 200:
        print("MAOER getdrama HTTP:", r1.status_code)
        print(r1.text[:300])
        r1.raise_for_status()
    j1 = r1.json()
    info1 = j1.get("info", {})
    drama = info1.get("drama", {}) if isinstance(info1, dict) else {}

    title = drama.get("name")
    cover_url = drama.get("cover")
    is_serial = bool(drama.get("serialize"))
    newest_title = drama.get("newest")

    # ② 再拿集数统计（不同返回结构兜底）
    total_count = None
    r2 = requests.get(MAOER_EPISODE_DETAILS, params={"drama_id": drama_id, "p": 1, "page_size": 10}, headers=headers, timeout=30)
    if r2.status_code != 200:
        print("MAOER episodedetails HTTP:", r2.status_code)
        print(r2.text[:300])
        r2.raise_for_status()

    j2 = r2.json()
    info2 = j2.get("info", {})

    # 结构 A：info.pagination.count
    if isinstance(info2, dict):
        pag = info2.get("pagination")
        if isinstance(pag, dict) and isinstance(pag.get("count"), int):
            total_count = pag["count"]

        # 结构 B：info.episodes.episode 列表
        if total_count is None:
            eps_block = info2.get("episodes")
            if isinstance(eps_block, dict):
                ep_list = eps_block.get("episode")
                if isinstance(ep_list, list):
                    total_count = len(ep_list)

        # 结构 C：info.Datas 列表
        if total_count is None:
            datas = info2.get("Datas")
            if isinstance(datas, list):
                # 这通常只是第一页，不等于总集数；但总比空强
                total_count = len(datas)

    now_iso = datetime.now(timezone.utc).isoformat()

    return {
        "platform": "猫耳",            # 默认猫耳
        "work_id": drama_id,
        "work_url": referer,
        "title": title,
        "cover_url": cover_url,
        "is_serial": is_serial,
        "newest_title": newest_title,
        "latest_count": total_count,
        "last_sync": now_iso,
    }


def notion_properties_for_work(data: dict) -> dict:
    props = {
        "Title": {"title": [{"text": {"content": data["title"] or f"猫耳-{data['work_id']}"}}]},
        "Platform": {"select": {"name": data["platform"]}},  # 直接写死猫耳
        "Work ID": {"rich_text": [{"text": {"content": str(data["work_id"])}}]},
        "Work URL": {"url": data.get("work_url")},
        "Cover URL": {"url": data.get("cover_url")},
        "Is Serial": {"checkbox": bool(data.get("is_serial"))},
        "Latest Episode": {"rich_text": [{"text": {"content": data.get("newest_title") or ""}}]},
        "Latest Episode No": {"number": data.get("latest_count") if isinstance(data.get("latest_count"), int) else None},
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

    cfg = load_works_from_json("works.json")
    urls: List[str] = cfg["urls"]

    for u in urls:
        data = maoer_fetch(u)
        key = f"{data['platform']}:{data['work_id']}"  # 仍然跟你 Notion 里的 Key 公式一致

        props = notion_properties_for_work(data)
        existing = notion_query_by_key(key)

        if existing:
            page_id = existing[0]["id"]
            notion_update_page(page_id, props)
            print("updated", data["work_id"], data.get("title"), data.get("newest_title"))
        else:
            notion_create_page(props)
            print("created", data["work_id"], data.get("title"), data.get("newest_title"))


if __name__ == "__main__":
    main()
