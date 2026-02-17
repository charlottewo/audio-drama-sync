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

# ====== Notion 字段名 ======
PROP_TITLE = "Title"
PROP_PLATFORM = "Platform"
PROP_WORK_URL = "Work URL"
PROP_WORK_ID = "Work ID"
PROP_IS_SERIAL = "Is Serial"
PROP_LATEST_EP = "Latest Episode"
PROP_LATEST_NO = "Latest Episode No"
PROP_LAST_SYNC = "Last Sync"
PROP_PRICE = "Price"
PROP_MAIN_CV = "Main CV"   # ⚠️ 如果不是这个名字，改成你的


# -------------------------
def notion_headers():
    return {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json",
    }


def extract_drama_id(work_url: str) -> Optional[int]:
    if not work_url:
        return None
    m = re.search(r"/mdrama/(\d+)", work_url)
    if m:
        return int(m.group(1))
    return None


def maoer_headers(referer_url: str):
    h = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/plain, */*",
        "Referer": referer_url,
        "Origin": "https://www.missevan.com",
    }
    if MISSEVAN_COOKIE:
        h["Cookie"] = MISSEVAN_COOKIE
    return h


def extract_main_cv(drama: dict) -> str:
    """
    抓前 4 位 CV
    优先 dramalist > author / staff
    """
    cvs = []

    dramalist = drama.get("dramatis_personae") or drama.get("dramalist")
    if isinstance(dramalist, list):
        for item in dramalist:
            name = item.get("name")
            if name:
                cvs.append(name)

    # 兜底：staff 里找 role 含 CV 的
    if not cvs:
        staff = drama.get("staff")
        if isinstance(staff, list):
            for s in staff:
                if "cv" in str(s.get("role", "")).lower():
                    name = s.get("name")
                    if name:
                        cvs.append(name)

    # 只保留前 4
    return " / ".join(cvs[:4])


def maoer_fetch_by_url(work_url: str) -> Dict[str, Any]:
    drama_id = extract_drama_id(work_url)
    if not drama_id:
        raise ValueError("无法解析 drama_id")

    referer = f"https://www.missevan.com/mdrama/{drama_id}"
    headers = maoer_headers(referer)

    # 基本信息
    r1 = requests.get(
        MAOER_GET_DRAMA,
        params={"drama_id": drama_id},
        headers=headers,
        timeout=30
    )
    r1.raise_for_status()
    j1 = r1.json()
    info1 = j1.get("info", {})
    drama = info1.get("drama", {})

    title = drama.get("name")
    cover_url = drama.get("cover")
    is_serial = bool(drama.get("serialize"))
    newest_title = drama.get("newest")
    price = drama.get("price")

    main_cv = extract_main_cv(drama)

    # 集数统计
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
        "price": price,
        "main_cv": main_cv,
        "last_sync": now_iso,
    }


def notion_db_query_all():
    url = f"https://api.notion.com/v1/databases/{NOTION_DB_ID}/query"
    r = requests.post(url, headers=notion_headers(), json={}, timeout=30)
    r.raise_for_status()
    return r.json().get("results", [])


def notion_update_page(page_id: str, properties: dict):
    url = f"https://api.notion.com/v1/pages/{page_id}"
    r = requests.patch(url, headers=notion_headers(), json={"properties": properties}, timeout=30)
    if r.status_code != 200:
        print("NOTION update failed:", r.text)
    r.raise_for_status()


def notion_props_from_data(data: dict) -> dict:
    return {
        PROP_TITLE: {"title": [{"text": {"content": data["title"] or f"猫耳-{data['work_id']}"}}]},
        PROP_PLATFORM: {"select": {"name": data["platform"]}},
        PROP_WORK_ID: {"rich_text": [{"text": {"content": str(data["work_id"])}}]},
        PROP_WORK_URL: {"url": data.get("work_url")},
        "Cover URL": {"url": data.get("cover_url")},
        PROP_IS_SERIAL: {"checkbox": bool(data.get("is_serial"))},
        PROP_LATEST_EP: {"rich_text": [{"text": {"content": data.get("newest_title") or ""}}]},
        PROP_LATEST_NO: {"number": data.get("latest_count")},
        PROP_PRICE: {"number": data.get("price") if isinstance(data.get("price"), (int, float)) else None},
        PROP_MAIN_CV: {"rich_text": [{"text": {"content": data.get("main_cv") or ""}}]},
        PROP_LAST_SYNC: {"date": {"start": data.get("last_sync")}},
    }


def main():
    pages = notion_db_query_all()
    print("NOTION pages:", len(pages))

    for p in pages:
        page_id = p["id"]
        work_url = p["properties"].get(PROP_WORK_URL, {}).get("url")

        if not work_url:
            continue

        try:
            data = maoer_fetch_by_url(work_url)
            props = notion_props_from_data(data)
            notion_update_page(page_id, props)
            print("updated", data["work_id"], data.get("title"))
        except Exception as e:
            print("FAILED:", work_url, "=>", e)


if __name__ == "__main__":
    main()
