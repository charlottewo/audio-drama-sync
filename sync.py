import os
import re
import requests
from datetime import datetime, timezone

NOTION_TOKEN = os.environ["NOTION_TOKEN"]
NOTION_DB_ID = os.environ["NOTION_DB_ID"]
MISSEVAN_COOKIE = os.environ.get("MISSEVAN_COOKIE", "")

GET_DRAMA = "https://www.missevan.com/dramaapi/getdrama"
GET_EPISODE_DETAILS = "https://www.missevan.com/dramaapi/getdramaepisodedetails"

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"


# ---------- Notion ----------
def notion_headers():
    return {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json",
    }


def notion_healthcheck():
    r = requests.get("https://api.notion.com/v1/users/me", headers=notion_headers(), timeout=30)
    print("NOTION /users/me:", r.status_code)
    if r.status_code != 200:
        print(r.text[:300])
    r.raise_for_status()


def notion_db_check():
    r = requests.get(f"https://api.notion.com/v1/databases/{NOTION_DB_ID}", headers=notion_headers(), timeout=30)
    print("NOTION /databases/{id}:", r.status_code)
    if r.status_code != 200:
        print(r.text[:300])
    r.raise_for_status()


def notion_query_by_key(key: str):
    url = f"https://api.notion.com/v1/databases/{NOTION_DB_ID}/query"
    body = {"filter": {"property": "Key", "formula": {"string": {"equals": key}}}}
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
    if r.status_code != 200:
        print("NOTION create failed:", r.status_code)
        print(r.text[:800])
    r.raise_for_status()


def _get_prop_text(prop: dict) -> str:
    """Read rich_text/title plain text from a Notion property."""
    if not prop:
        return ""
    t = prop.get("type")
    if t == "rich_text":
        return "".join([x.get("plain_text", "") for x in prop.get("rich_text", [])])
    if t == "title":
        return "".join([x.get("plain_text", "") for x in prop.get("title", [])])
    if t == "url":
        return prop.get("url") or ""
    if t == "number":
        v = prop.get("number")
        return "" if v is None else str(int(v)) if float(v).is_integer() else str(v)
    if t == "select":
        s = prop.get("select")
        return (s or {}).get("name", "") if s else ""
    return ""


def notion_list_maoer_rows():
    """
    从 Notion 数据库拉取 Platform=猫耳 的所有行。
    你只需要在 Notion 里新增一行并填 Platform + Work URL/Work ID，就会自动纳入同步。
    """
    url = f"https://api.notion.com/v1/databases/{NOTION_DB_ID}/query"
    body = {"page_size": 100, "filter": {"property": "Platform", "select": {"equals": "猫耳"}}}

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
            rows.append(
                {
                    "page_id": page_id,
                    "work_url": work_url,
                    "work_id_text": work_id_text,
                }
            )

        if not data.get("has_more"):
            break
        next_cursor = data.get("next_cursor")

    return rows


# ---------- Maoer / MissEvan ----------
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
    print("MAOER status:", r.status_code)
    print("MAOER content-type:", r.headers.get("content-type", ""))
    if r.status_code != 200:
        print("MAOER head:", r.text[:300])
    r.raise_for_status()
    j = r.json()
    info = j.get("info", {})
    return {"info": info or {}}


def format_cvs(cvs: list, limit: int = 10) -> str:
    out = []
    for item in cvs[:limit]:
        chara = (item.get("character") or "").strip()
        cv = (item.get("cv_info") or {}).get("name") or ""
        grp = (item.get("cv_info") or {}).get("group") or ""
        if not (chara or cv):
            continue
        if grp:
            out.append(f"{chara} - {cv}({grp})" if chara else f"{cv}({grp})")
        else:
            out.append(f"{chara} - {cv}" if chara else cv)
    return "; ".join(out)


def maoer_fetch(work_id: int) -> dict:
    meta = maoer_get_drama(work_id)          # 负责：剧名/封面/价格/连载/新更/CV
    detail = maoer_get_episode_details(work_id)  # 负责：集数统计（用 pagination.count 更稳）

    drama = meta.get("drama", {})
    cvs = meta.get("cvs", [])
    info = detail.get("info", {})

    title = drama.get("name")
    cover_url = drama.get("cover")
    price = drama.get("price")
    is_serial = bool(drama.get("serialize"))
    newest_title = drama.get("newest")

    # 集数：优先用 pagination.count（总条数），其次才用列表长度
    latest_count = None
    if isinstance(info, dict):
        pag = info.get("pagination", {})
        if isinstance(pag, dict) and pag.get("count") is not None:
            latest_count = pag.get("count")
        else:
            datas = info.get("Datas", [])
            if isinstance(datas, list) and datas:
                latest_count = len(datas)

    now_iso = datetime.now(timezone.utc).isoformat()

    return {
        "title": title,
        "cover_url": cover_url,
        "price": price,
        "is_serial": is_serial,
        "newest_title": newest_title,
        "latest_count": latest_count,
        "cv_text": format_cvs(cvs, limit=12),
        "last_sync": now_iso,
    }


def parse_work_id(work_url: str, fallback: str) -> int | None:
    # 兼容两种常见路径：/mdrama/91093 或 /mdrama/drama/26870
    m = re.search(r"/mdrama/(?:drama/)?(\d+)", work_url or "")
    if m:
        return int(m.group(1))
    if fallback and fallback.isdigit():
        return int(fallback)
    return None


def notion_properties_for_work(work_id: int, work_url: str, data: dict):
    props = {
        "Title": {"title": [{"text": {"content": data.get("title") or f"猫耳-{work_id}"}}]},
        "Platform": {"select": {"name": "猫耳"}},
        # 你 Work ID 如果做成 number 也行；你现在是 rich_text，就按 rich_text 写
        "Work ID": {"rich_text": [{"text": {"content": str(work_id)}}]},
        "Work URL": {"url": work_url or f"https://www.missevan.com/mdrama/{work_id}"},
        "Cover URL": {"url": data.get("cover_url")},
        "Price": {"number": data.get("price")},
        "Is Serial": {"checkbox": bool(data.get("is_serial"))},
        "Latest Episode": {"rich_text": [{"text": {"content": data.get("newest_title") or ""}}]},
        "Latest Episode No": {"number": data.get("latest_count")},
        "Last Sync": {"date": {"start": data.get("last_sync")}},
    }

    # 如果你在 Notion 里新增了 CV 字段（rich_text），就会自动写进去
    if data.get("cv_text"):
        props["CV"] = {"rich_text": [{"text": {"content": data["cv_text"]}}]}

    return props


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
            print("skip: cannot parse Work ID from row", page_id, work_url, row["work_id_text"])
            continue

        data = maoer_fetch(work_id)
        props = notion_properties_for_work(work_id, work_url, data)

        # 这行已存在，直接 update
        notion_update_page(page_id, props)
        print("updated", work_id, data.get("title"), data.get("newest_title"))


if __name__ == "__main__":
    main()
