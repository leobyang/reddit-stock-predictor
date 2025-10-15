# phase2_sentiment.py
import os
import pandas as pd
from datetime import datetime, timezone, timedelta

import psycopg2
import psycopg2.extras as extras

from nltk.sentiment import SentimentIntensityAnalyzer

from dotenv import load_dotenv
load_dotenv(dotenv_path="/Users/leoyang/Downloads/reddit-stock-predictor/apicreds.env")

PG_CONN_KW = dict(
    host=os.getenv("PGHOST", "localhost"),
    port=os.getenv("PGPORT", "5432"),
    dbname=os.getenv("PGDATABASE", "reddit_sentiment"),
    user=os.getenv("PGUSER"),
    password=os.getenv("PGPASSWORD"),
)

USE_FINBERT = False  # flip True to run FinBERT
FINBERT_MODEL = "ProsusAI/finbert"

# ---------- DB helpers ----------
def get_conn():
    return psycopg2.connect(**PG_CONN_KW)

# Pull unscored (or to-be-rescored) items
POSTS_SQL = """
SELECT p.id, p.score, p.created_utc, COALESCE(p.title,'') || ' ' || COALESCE(p.selftext,'') AS text
FROM posts p
LEFT JOIN post_sentiment s ON s.post_id = p.id
WHERE (%(rescore)s = TRUE AND p.created_utc >= %(since)s)
   OR (%(rescore)s = FALSE AND s.post_id IS NULL)
"""

COMMENTS_SQL = """
SELECT c.id, c.score, c.created_utc, COALESCE(c.body,'') AS text
FROM comments c
LEFT JOIN comment_sentiment s ON s.comment_id = c.id
WHERE (%(rescore)s = TRUE AND c.created_utc >= %(since)s)
   OR (%(rescore)s = FALSE AND s.comment_id IS NULL)
"""

def fetch_for_scoring(conn, table: str, rescore=False, since_days=14) -> pd.DataFrame:
    params = {"rescore": rescore, "since": datetime.now(tz=timezone.utc) - timedelta(days=since_days)}
    sql = POSTS_SQL if table == "posts" else COMMENTS_SQL
    return pd.read_sql(sql, conn, params=params)

# ---------- VADER ----------
_vader = SentimentIntensityAnalyzer()

def score_vader(texts):
    # returns dicts with pos/neg/neu/compound
    return [_vader.polarity_scores(t or "") for t in texts]

# ---------- FinBERT (optional) ----------
_pipeline = None
def score_finbert(texts):
    global _pipeline
    if _pipeline is None:
        from transformers import AutoTokenizer, AutoModelForSequenceClassification, TextClassificationPipeline
        tok = AutoTokenizer.from_pretrained(FINBERT_MODEL)
        mdl = AutoModelForSequenceClassification.from_pretrained(FINBERT_MODEL)
        _pipeline = TextClassificationPipeline(model=mdl, tokenizer=tok, truncation=True, max_length=256, device=-1)
    preds = _pipeline(list(texts))
    # map to signed
    out = []
    for p in preds:
        label = p["label"].lower()
        conf = float(p["score"])
        signed = {"positive": 1.0, "neutral": 0.0, "negative": -1.0}.get(label, 0.0) * conf
        out.append((label, conf, signed))
    return out

# ---------- bulk upserts ----------
def upsert_post_sentiment(cur, rows):
    # rows: list of tuples (post_id, vader_pos, vader_neg, vader_neu, vader_compound, finbert_label, finbert_conf, finbert_signed)
    sql = """
    INSERT INTO post_sentiment (post_id, vader_pos, vader_neg, vader_neu, vader_compound, finbert_label, finbert_conf, finbert_signed)
    VALUES %s
    ON CONFLICT (post_id) DO UPDATE SET
      vader_pos=EXCLUDED.vader_pos,
      vader_neg=EXCLUDED.vader_neg,
      vader_neu=EXCLUDED.vader_neu,
      vader_compound=EXCLUDED.vader_compound,
      finbert_label=EXCLUDED.finbert_label,
      finbert_conf=EXCLUDED.finbert_conf,
      finbert_signed=EXCLUDED.finbert_signed,
      scored_at=now();
    """
    extras.execute_values(cur, sql, rows, page_size=500)

def upsert_comment_sentiment(cur, rows):
    sql = """
    INSERT INTO comment_sentiment (comment_id, vader_pos, vader_neg, vader_neu, vader_compound, finbert_label, finbert_conf, finbert_signed)
    VALUES %s
    ON CONFLICT (comment_id) DO UPDATE SET
      vader_pos=EXCLUDED.vader_pos,
      vader_neg=EXCLUDED.vader_neg,
      vader_neu=EXCLUDED.vader_neu,
      vader_compound=EXCLUDED.vader_compound,
      finbert_label=EXCLUDED.finbert_label,
      finbert_conf=EXCLUDED.finbert_conf,
      finbert_signed=EXCLUDED.finbert_signed,
      scored_at=now();
    """
    extras.execute_values(cur, sql, rows, page_size=1000)

# ---------- main scoring routines ----------
def score_table(conn, table: str, use_finbert=False, rescore=False):
    df = fetch_for_scoring(conn, table, rescore=rescore)
    if df.empty:
        print(f"[{table}] nothing to score.")
        return 0

    # VADER
    v = score_vader(df["text"])
    df["vader_pos"] = [d["pos"] for d in v]
    df["vader_neg"] = [d["neg"] for d in v]
    df["vader_neu"] = [d["neu"] for d in v]
    df["vader_compound"] = [d["compound"] for d in v]

    # FinBERT (optional)
    if use_finbert:
        labels, confs, signed = zip(*score_finbert(df["text"]))
        df["finbert_label"] = labels
        df["finbert_conf"]  = confs
        df["finbert_signed"]= signed
    else:
        df["finbert_label"] = None
        df["finbert_conf"]  = None
        df["finbert_signed"]= None

    tuples = []
    if table == "posts":
        tuples = list(
            zip(df["id"], df["vader_pos"], df["vader_neg"], df["vader_neu"], df["vader_compound"],
                df["finbert_label"], df["finbert_conf"], df["finbert_signed"])
        )
        with conn, conn.cursor() as cur:
            upsert_post_sentiment(cur, tuples)
    else:
        tuples = list(
            zip(df["id"], df["vader_pos"], df["vader_neg"], df["vader_neu"], df["vader_compound"],
                df["finbert_label"], df["finbert_conf"], df["finbert_signed"])
        )
        with conn, conn.cursor() as cur:
            upsert_comment_sentiment(cur, tuples)

    print(f"[{table}] scored {len(tuples)} rows.")
    return len(tuples)

# ---------- daily aggregation ----------
AGG_SQL = """
-- Aggregate post sentiment + comment sentiment by ticker + UTC date (weighted by score).
WITH post_s AS (
  SELECT pt.ticker_id, t.symbol AS ticker,
         (p.created_utc AT TIME ZONE 'UTC')::date AS d,
         COALESCE(ps.finbert_signed, ps.vader_compound) AS s,
         GREATEST(p.score, 0) AS w
  FROM posts p
  JOIN post_tickers pt   ON pt.post_id = p.id
  JOIN tickers t         ON t.id = pt.ticker_id
  JOIN post_sentiment ps ON ps.post_id = p.id
),
comment_s AS (
  SELECT ct.ticker_id, t.symbol AS ticker,
         (c.created_utc AT TIME ZONE 'UTC')::date AS d,
         COALESCE(cs.finbert_signed, cs.vader_compound) AS s,
         GREATEST(c.score, 0) AS w
  FROM comments c
  JOIN comment_tickers ct   ON ct.comment_id = c.id
  JOIN tickers t            ON t.id = ct.ticker_id
  JOIN comment_sentiment cs ON cs.comment_id = c.id
),
union_s AS (
  SELECT ticker, d AS date, s, w FROM post_s
  UNION ALL
  SELECT ticker, d AS date, s, w FROM comment_s
),
rolled AS (
  SELECT ticker, date,
         SUM(s * w) / NULLIF(SUM(w),0) AS sentiment,
         COUNT(*) AS count,
         SUM(w) AS weight_sum
  FROM union_s
  GROUP BY ticker, date
)
INSERT INTO daily_sentiment (ticker, date, sentiment, count, weight_sum)
SELECT ticker, date, sentiment, count, weight_sum
FROM rolled
ON CONFLICT (ticker, date)
DO UPDATE SET
  sentiment  = EXCLUDED.sentiment,
  count      = EXCLUDED.count,
  weight_sum = EXCLUDED.weight_sum;
"""

def aggregate_daily(conn):
    with conn, conn.cursor() as cur:
        cur.execute(AGG_SQL)
    print("[daily] aggregated.")

# ---------- CLI entry ----------
if __name__ == "__main__":
    conn = get_conn()
    try:
        # Score new/unscored rows (set rescore=True to refresh last N days)
        score_table(conn, "posts",   use_finbert=USE_FINBERT, rescore=False)
        score_table(conn, "comments",use_finBERT:=USE_FINBERT, rescore=False)  # py>=3.8 ok with walrus

        # Build/refresh daily rollups
        aggregate_daily(conn)
    finally:
        conn.close()
