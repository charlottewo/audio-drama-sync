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
    """Notion page-level cover payload (external image url)."""
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
    if t == "number":
        v = prop.get("number")
        return "" if v is None else str(v)
    return ""


def notion_list_maoer_rows():
    """
    拉取 Platform=猫耳 的所有行。
    新增剧集：只要新建一行，Platform选猫耳，填 Work URL 或 Work ID。
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

            work_url = _get_prop_text(props.get("Work URL")).strip()
            work_id_text = _get_prop_text(props.get("Work ID")).strip()
            main_cv_override = _get_prop_text(props.get("Main CV Override")).strip()

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
    body = {"properties": properties}
    cover = notion_cover_payload(cover_url)
    if cover:
        body["cover"] = cover

    r = requests.patch(url, headers=notion_headers(), json=body, timeout=30)
    if r.status_code != 200:
        print("NOTION update failed:", r.status_code)
        print(r.text[:800])
    r.raise_for_status()


def notion_create_page(properties: dict, cover_url: str | None = None):
    url = "https://api.notion.com/v1/pages"

    body = {"parent": {"database_id": NOTION_DB_ID}, "properties": properties}
    cover = notion_cover_payload(cover_url)
    if cover:
        body["cover"] = cover

    r = requests.post(url, headers=notion_headers(), json=body, timeout=30)
    if r.status_code != 200:
        print("NOTION create failed:", r.status_code)
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
    cvs = info.get("cvs", []) if isinstance(info, dict) else []
    return {"drama": drama or {}, "cvs": cvs or []}


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


# =========================
# CV picking (A) + Override (B)
# =========================
BAD_WORDS_STRONG = [
    "导演", "监制", "制作", "策划", "编剧", "后期", "统筹", "录音", "配音导演",
    "旁白", "报幕", "字幕", "美工", "宣发", "运营", "音效", "混音", "母带",
]

MUSIC_WORDS_STRONG = [
    "演唱", "主题曲", "片尾曲", "插曲", "作词", "作曲", "编曲", "和声", "歌曲", "OST"
]


def pick_main_cvs(cvs: list, k: int = 4) -> str:
    candidates = []

    for item in cvs or []:
        character = (item.get("character") or "").strip()
        cv_info = item.get("cv_info") or {}
        name = (cv_info.get("name") or "").strip()
        group = (cv_info.get("group") or "").strip()

        if not name:
            continue

        # 1) 强过滤：音乐标签直接踢
        if any(w in character for w in MUSIC_WORDS_STRONG):
            continue

        # 2) 过滤：制作/旁白等
        if any(w in character for w in BAD_WORDS_STRONG):
            continue

        # 3) 打分：越像“纯角色名”越高
        score = 0
        if character:
            score += 20
            if "/" in character:
                score -= 4
            if len(character) > 10:
                score -= 3
        else:
            score -= 10

        candidates.append((score, character, name, group))

    # 兜底：过滤太狠导致不足 k 个时，退一步但仍排除音乐/制作
    if len(candidates) < k:
        for item in cvs or []:
            character = (item.get("character") or "").strip()
            cv_info = item.get("cv_info") or {}
            name = (cv_info.get("name") or "").strip()
            group = (cv_info.get("group") or "").strip()

            if not name:
                continue
            if any(w in character for w in MUSIC_WORDS_STRONG):
                continue
            if any(w in character for w in BAD_WORDS_STRONG):
                continue

            candidates.append((5, character, name, group))

    candidates.sort(key=lambda x: x[0], reverse=True)
    top = candidates[:k]

    if not top:
        return ""

    out = []
    for _, character, name, group in top:
        left = character if character else "角色未标注"
        out.append(f"{left} - {name}{f'({group})' if group else ''}")

    return "; ".join(out)


# =========================
# Fetch + parse
# =========================
def maoer_fetch(work_id: int) -> dict:
    meta = maoer_get_drama(work_id)               # 剧名/封面/价格/连载/新更/CV
    detail = maoer_get_episode_details(work_id)   # 集数统计（pagination.count）

    drama = meta.get("drama", {})
    cvs = meta.get("cvs", [])
    info = detail.get("info", {})

    title = drama.get("name")
    cover_url = drama.get("cover")
    price = drama.get("price")
    is_serial = bool(drama.get("serialize"))
    newest_title = drama.get("newest")

    # 集数：优先 pagination.count（总条数）
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
        "cv_text": pick_main_cvs(cvs, k=4),  # ← 默认写前4位
        "last_sync": now_iso,
    }


def parse_work_id(work_url: str, fallback: str):
    """
    兼容：
    - https://www.missevan.com/mdrama/91093
    - https://www.missevan.com/mdrama/drama/26870
    - 只要 URL 里出现 /mdrama/数字 或 /mdrama/drama/数字 都能抓出来
    """
    u = (work_url or "").strip()
    m = re.search(r"/mdrama/(?:drama/)?(\d+)", u)
    if m:
        return int(m.group(1))

    if fallback and fallback.isdigit():
        return int(fallback)

    return None


def notion_properties_for_work(work_id: int, work_url: str, data: dict):
    props = {
        "Title": {"title": [{"text": {"content": data.get("title") or f"猫耳-{work_id}"}}]},
        "Platform": {"select": {"name": "猫耳"}},
        "Work ID": {"rich_text": [{"text": {"content": str(work_id)}}]},
        "Work URL": {"url": work_url or f"https://www.missevan.com/mdrama/{work_id}"},
        "Cover URL": {"url": data.get("cover_url")},
        "Price": {"number": data.get("price")},
        "Is Serial": {"checkbox": bool(data.get("is_serial"))},
        "Latest Episode": {"rich_text": [{"text": {"content": data.get("newest_title") or ""}}]},
        "Latest Episode No": {"number": data.get("latest_count")},
        "Last Sync": {"date": {"start": data.get("last_sync")}},
        "Cover URL": {"url": data.get("cover_url")},
    }

    # CV 字段（rich_text）
    if data.get("cv_text"):
        props["CV"] = {"rich_text": [{"text": {"content": data["cv_text"]}}]}

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
            print("  Tip: 确保 Platform=猫耳，并且 Work URL 含 /mdrama/数字")
            continue

        data = maoer_fetch(work_id)

        # B：Notion 手动兜底优先（你填了就永远用你填的）
        override = (row.get("main_cv_override") or "").strip()
        if override:
            data["cv_text"] = override

        props = notion_properties_for_work(work_id, work_url, data)

        # 自动更新：properties + Page cover
        notion_update_page(page_id, props, cover_url=data.get("cover_url"))

        print("updated", work_id, data.get("title"), f"count={data.get('latest_count')}", f"serial={data.get('is_serial')}")

if __name__ == "__main__":
    main()
