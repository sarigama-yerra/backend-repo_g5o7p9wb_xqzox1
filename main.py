import os
import csv
import io
from datetime import datetime
from typing import List, Dict, Any, Optional
from collections import Counter, defaultdict

import requests
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Netflix Analytics API (no-pandas)")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DATA_URLS = [
    "https://raw.githubusercontent.com/erikgregorywebb/datasets/master/netflix_titles.csv",
    "https://raw.githubusercontent.com/ashishpatel26/EDA-on-Netflix-Dataset/master/netflix_titles.csv",
    "https://raw.githubusercontent.com/singhaniatanay/Netflix-Data-Analysis/master/netflix_titles.csv",
]

_cache_rows: Optional[List[Dict[str, Any]] ] = None


def _parse_date(val: str) -> Optional[datetime]:
    if not val:
        return None
    for fmt in ("%B %d, %Y", "%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(val, fmt)
        except Exception:
            continue
    return None


def _split_list(val: str) -> List[str]:
    if not val:
        return []
    return [p.strip() for p in val.split(',') if p.strip()]


def load_dataset() -> List[Dict[str, Any]]:
    global _cache_rows
    if _cache_rows is not None:
        return _cache_rows

    local_path = os.getenv("NETFLIX_CSV_PATH", "netflix_titles.csv")
    csv_text: Optional[str] = None

    if os.path.exists(local_path):
        with open(local_path, "r", encoding="utf-8") as f:
            csv_text = f.read()
    else:
        last_err: Optional[Exception] = None
        for url in DATA_URLS:
            try:
                resp = requests.get(url, timeout=20)
                resp.raise_for_status()
                csv_text = resp.text
                break
            except Exception as e:
                last_err = e
        if csv_text is None:
            raise HTTPException(status_code=500, detail=f"Could not load dataset: {last_err}")

    rows: List[Dict[str, Any]] = []
    reader = csv.DictReader(io.StringIO(csv_text))
    for r in reader:
        # Normalize fields with defaults
        show_id = r.get("show_id") or ""
        ttype = (r.get("type") or "").strip()
        title = (r.get("title") or "").strip()
        director = r.get("director") or ""
        cast = r.get("cast") or ""
        country = r.get("country") or ""
        date_added = r.get("date_added") or ""
        try:
            release_year = int(r.get("release_year") or 0) or None
        except Exception:
            release_year = None
        rating = r.get("rating") or ""
        duration = r.get("duration") or ""
        listed_in = r.get("listed_in") or ""
        description = r.get("description") or ""

        rows.append({
            "show_id": show_id,
            "type": ttype,
            "title": title,
            "director": director,
            "cast": cast,
            "country": country,
            "date_added": _parse_date(date_added),
            "release_year": release_year,
            "rating": rating,
            "duration": duration,
            "listed_in": listed_in,
            "description": description,
            "countries_list": _split_list(country),
            "genres_list": _split_list(listed_in),
        })

    _cache_rows = rows
    return rows


@app.get("/")
def root() -> Dict[str, str]:
    return {"message": "Netflix Analytics API is running"}


@app.get("/api/netflix/summary")
def summary() -> Dict[str, Any]:
    rows = load_dataset()
    total = len(rows)
    movies = sum(1 for r in rows if r["type"].upper() == "MOVIE")
    shows = sum(1 for r in rows if r["type"].upper() == "TV SHOW")

    years = [r["release_year"] for r in rows if isinstance(r["release_year"], int)]
    min_year = min(years) if years else None
    max_year = max(years) if years else None

    dates = [r["date_added"] for r in rows if isinstance(r["date_added"], datetime)]
    first_added = min(dates).strftime("%Y-%m-%d") if dates else None
    last_added = max(dates).strftime("%Y-%m-%d") if dates else None

    return {
        "total_titles": total,
        "movies": movies,
        "tv_shows": shows,
        "earliest_release_year": min_year,
        "latest_release_year": max_year,
        "first_added_date": first_added,
        "last_added_date": last_added,
    }


@app.get("/api/netflix/by-country")
def by_country(top: int = Query(10, ge=1, le=50)) -> List[Dict[str, Any]]:
    rows = load_dataset()
    counter: Counter[str] = Counter()
    for r in rows:
        for c in r["countries_list"]:
            counter[c] += 1
    items = counter.most_common(top)
    return [{"country": k, "count": v} for k, v in items]


@app.get("/api/netflix/by-genre")
def by_genre(top: int = Query(15, ge=1, le=100)) -> List[Dict[str, Any]]:
    rows = load_dataset()
    counter: Counter[str] = Counter()
    for r in rows:
        for g in r["genres_list"]:
            counter[g] += 1
    items = counter.most_common(top)
    return [{"genre": k, "count": v} for k, v in items]


@app.get("/api/netflix/by-year")
def by_year(start: Optional[int] = None, end: Optional[int] = None) -> List[Dict[str, Any]]:
    rows = load_dataset()
    counter: defaultdict[int, int] = defaultdict(int)
    for r in rows:
        y = r.get("release_year")
        if isinstance(y, int):
            if start is not None and y < start:
                continue
            if end is not None and y > end:
                continue
            counter[y] += 1
    return [{"year": y, "count": counter[y]} for y in sorted(counter.keys())]


@app.get("/api/netflix/search")
def search(q: str = Query("", min_length=0), type: Optional[str] = Query(None)) -> List[Dict[str, Any]]:  # noqa: A002
    rows = load_dataset()
    ql = q.lower().strip()
    out: List[Dict[str, Any]] = []
    for r in rows:
        if type and r["type"].strip().upper() != type.strip().upper():
            continue
        if ql:
            hay = " ".join([
                r.get("title", ""), r.get("director", ""), r.get("cast", ""), r.get("listed_in", ""), r.get("description", "")
            ]).lower()
            if ql not in hay:
                continue
        out.append({
            "show_id": r["show_id"],
            "title": r["title"],
            "type": r["type"],
            "country": r["country"],
            "release_year": r["release_year"],
            "rating": r["rating"],
            "duration": r["duration"],
            "listed_in": r["listed_in"],
            "date_added": r["date_added"].strftime("%Y-%m-%d") if r["date_added"] else None,
            "description": r["description"],
        })
        if len(out) >= 25:
            break
    return out


@app.get("/test")
def test_database():
    response: Dict[str, Any] = {
        "backend": "✅ Running",
        "database": "❌ Not Used (CSV analytics)",
        "database_url": "✅ Set" if os.getenv("DATABASE_URL") else "❌ Not Set",
        "database_name": "✅ Set" if os.getenv("DATABASE_NAME") else "❌ Not Set",
        "connection_status": "Not Connected",
        "collections": [],
    }
    return response


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
