import os
import re
import requests
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List, Iterator

NOTION_TOKEN = os.environ["NOTION_TOKEN"]
NOTION_DB_ID = os.environ["NOTION_DB_ID"]
MISSEVAN_COOKIE = os.environ.get("MISSEVAN_COOKIE", "").strip()

MAOER_GET_DRAMA = "https://www.missevan.com/dramaapi/getdrama"
MAOER_EPISODE_DETAILS = "https://www.missevan.com/dramaapi/getdramaepisodedetails"

# ====== Notion 字段名（必须与你数据库一致）======
PROP_TITLE = "Title"
PROP_PLATFORM = "Platform"
PROP_WORK_ID = "Work ID"
PROP_WORK_URL = "Work URL"
PROP_COVER_URL = "Cover URL"
PROP_IS_SERIAL = "Is Serial"
PROP_LATEST_EP = "Latest Episode"
PROP_LATEST_NO = "Latest Episode No"
PROP_LAST_SYNC = "Last Sync"
PROP_KEY = "Key"  # 你数据库里是 formula：format(prop("Platform")) + ":" + prop("Work ID")


def notion_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json",
    }


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
    return None


def maoer_headers(referer_url: str) -> Dict[str, str]:
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


def maoer_fetch(work_url: str) -> Dict[str, Any]:
    drama_id = extract_drama_id(work_url)
    if not drama_id:
        raise ValueError(f"无法从 URL 提取 drama_id：{work_url}")

    referer = f"https://www.missevan.com/mdrama/{drama_id}"
    headers = maoer_headers(referer)

    # ① 基本信息
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

    # ② 集数统计（尽量拿总数）
    total_count = None
    r2 = requests.get(
        MAOER_EPISODE_DETAILS,
        params={"drama_id": drama_id, "p": 1, "page_size": 10},
        headers=headers,
        timeout=30,
    )
    if r2.status_code != 200:
        print("MAOER episodedetails HTTP:", r2.status_code)
        print(r2.text[:300])
        r2.raise_for_status()

    j2 = r2.json()
    info2 = j2.get("info", {})

    # 优先：info.pagination.count（这是“总条数”）
    if isinstance(info2, dict):
        pag = info2.get("pagination")
        if isinstance(pag, dict) and isinstance(pag.get("count"), int):
            total_count = pag["count"]

        # 兜底 A：info.episodes.episode 列表长度
        if total_count is None:
            eps_block = info2.get("episodes")
            if isinstance(eps_block, dict):
                ep_list = eps_block.get("episode")
                if isinstance(ep_list, list):
                    total_count = len(ep_list)

        # 兜底 B：info.Datas 列表长度（通常只是第一页，不等于总数，但比空强）
        if total_count is None:
            datas = info2.get("Datas")
            if isinstance(datas, list):
                total_count = len(datas)

    now_iso = datetime.now(timezone.utc).isoformat()

    return {
        "platform": "猫耳",  # 你要的：不手动选平台，直接默认猫耳
        "work_id": drama_id,
        "work_url": referer,  # 统一写成标准详情页
        "title": title,
        "cover_url": cover_url,
        "is_serial": is_serial,
        "newest_title": newest_title,
        "latest_count": total_count,
        "last_sync": now_iso,
    }


def notion_properties_for_work(data: dict) -> dict:
    return {
        PROP_TITLE: {"title": [{"text": {"content": data["title"] or f"猫耳-{data['work_id']}"}}]},
        PROP_PLATFORM: {"select": {"name": data["platform"]}},
        PROP_WORK_ID: {"rich_text": [{"text": {"content": str(data["work_id"])}}]},
        PROP_WORK_URL: {"url": data.get("work_url")},
        PROP_COVER_URL: {"url": data.get("cover_url")},
        PROP_IS_SERIAL: {"checkbox": bool(data.get("is_serial"))},
        PROP_LATEST_EP: {"rich_text": [{"text": {"content": data.get("newest_title") or ""}}]},
        PROP_LATEST_NO: {"number": data.get("latest_count") if isinstance(data.get("latest_count"), int) else None},
        PROP_LAST_SYNC: {"date": {"start": data.get("last_sync")}},
    }


def notion_query_pages_with_work_url() -> Iterator[dict]:
    """
    扫描整个数据库，找出 Work URL 不为空的页面（自动分页）
    """
    url = f"https://api.notion.com/v1/databases/{NOTION_DB_ID}/query"
    payload: Dict[str, Any] = {
        "filter": {
            "property": PROP_WORK_URL,
            "url": {"is_not_empty": True},
        },
        "page_size": 100,
    }

    while True:
        r = requests.post(url, headers=notion_headers(), json=payload, timeout=30)
        if r.status_code != 200:
            print("NOTION query failed:", r.status_code)
            print(r.text[:1000])
            r.raise_for_status()

        data = r.json()
        results = data.get("results", [])
        for page in results:
            yield page

        if data.get("has_more"):
            payload["start_cursor"] = data.get("next_cursor")
        else:
            break


def notion_update_page(page_id: str, properties: dict):
    url = f"https://api.notion.com/v1/pages/{page_id}"
    body = {"properties": properties}
    r = requests.patch(url, headers=notion_headers(), json=body, timeout=30)
    if r.status_code != 200:
        print("NOTION update failed:", r.status_code)
        print(r.text[:1000])
        r.raise_for_status()


def safe_get_work_url(page: dict) -> str:
    props = page.get("properties", {})
    work_url_prop = props.get(PROP_WORK_URL, {})
    # url 类型：{"type":"url","url":"..."}
    u = work_url_prop.get("url")
    return u or ""


def main():
    notion_healthcheck()
    notion_db_check()

    pages = list(notion_query_pages_with_work_url())
    print("NOTION pages with Work URL:", len(pages))

    for page in pages:
        page_id = page["id"]
        raw_url = safe_get_work_url(page).strip()

        if not raw_url:
            continue

        # 只处理猫耳（missevan）
        if "missevan.com" not in raw_url:
            print("skip (not missevan):", raw_url)
            continue

        try:
            data = maoer_fetch(raw_url)
            props = notion_properties_for_work(data)
            notion_update_page(page_id, props)
            print("updated", data["work_id"], data.get("title"), data.get("newest_title"))
        except Exception as e:
            print("FAILED:", raw_url, "=>", repr(e))


if __name__ == "__main__":
    main()
