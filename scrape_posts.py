import os, re, json
from datetime import datetime, timezone

import praw
import psycopg2
import psycopg2.extras as extras
from dotenv import load_dotenv

load_dotenv(dotenv_path="/Users/leoyang/Downloads/reddit-stock-predictor/apicreds.env")

# --- Config ---
TARGET_SUBREDDITS = ["stocks", "wallstreetbets", "investing"]
# Simple pattern; later you can filter against a whitelist of symbols
TICKER_RE = re.compile(r'\b[A-Z]{1,5}\b')

# --- Reddit client ---
reddit = praw.Reddit(
    client_id=os.getenv("REDDIT_CLIENT_ID"),
    client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
    user_agent=os.getenv("REDDIT_USER_AGENT"),
)

# --- Postgres conn ---
conn = psycopg2.connect(
    host=os.getenv("PGHOST", "localhost"),
    port=os.getenv("PGPORT", "5432"),
    dbname=os.getenv("PGDATABASE", "reddit_sentiment"),
    user=os.getenv("PGUSER"),
    password=os.getenv("PGPASSWORD")
)
conn.autocommit = False  # we’ll commit in batches

def upsert_ref(cur, table, unique_col, value):
    sql = f"""
    INSERT INTO {table} ({unique_col}) VALUES (%s)
    ON CONFLICT ({unique_col}) DO UPDATE SET {unique_col}=EXCLUDED.{unique_col}
    RETURNING id;
    """
    cur.execute(sql, (value,))
    return cur.fetchone()[0]

def upsert_post(cur, p, subreddit_id, author_id):
    sql = """
    INSERT INTO posts (reddit_id, subreddit_id, author_id, title, selftext, url, permalink,
                       score, num_comments, created_utc, raw_json)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (reddit_id) DO UPDATE SET
      score = EXCLUDED.score,
      num_comments = EXCLUDED.num_comments
    RETURNING id;
    """
    created = datetime.fromtimestamp(p.created_utc, tz=timezone.utc)
    raw = {
        "id": p.id, "subreddit": str(p.subreddit), "author": str(p.author),
        "title": p.title, "selftext": p.selftext, "url": p.url,
        "permalink": p.permalink, "score": p.score, "num_comments": p.num_comments
    }
    cur.execute(sql, (
        p.id, subreddit_id, author_id, p.title or "", p.selftext or "", p.url or "",
        p.permalink or "", int(p.score or 0), int(p.num_comments or 0), created, json.dumps(raw)
    ))
    return cur.fetchone()[0]

def extract_tickers(text):
    if not text:
        return set()
    # naive: pull ALL 1-5 uppercase tokens; refine later with a real symbol list
    return set(t for t in TICKER_RE.findall(text) if t.isalpha())

def link_post_tickers(cur, post_id, syms):
    for sym in syms:
        tid = upsert_ref(cur, "tickers", "symbol", sym)
        cur.execute("""
            INSERT INTO post_tickers (post_id, ticker_id)
            VALUES (%s, %s) ON CONFLICT DO NOTHING;
        """, (post_id, tid))

def scrape_subreddit(name, limit=200):
    print(f"Scraping r/{name}…")
    with conn, conn.cursor() as cur:
        subreddit_id = upsert_ref(cur, "subreddits", "name", name)

        # hot() gives a good sample; you can use new(), top(time_filter="day"), etc.
        for p in reddit.subreddit(name).hot(limit=limit):
            author_name = f"u_{p.author.name}" if getattr(p.author, "name", None) else "u_deleted"
            author_id = upsert_ref(cur, "authors", "username", author_name)

            post_id = upsert_post(cur, p, subreddit_id, author_id)

            syms = extract_tickers((p.title or "") + " " + (p.selftext or ""))
            if syms:
                link_post_tickers(cur, post_id, syms)

if __name__ == "__main__":
    try:
        for s in TARGET_SUBREDDITS:
            scrape_subreddit(s)
        conn.commit()
        print("Done.")
    except Exception as e:
        conn.rollback()
        raise
    finally:
        conn.close()
