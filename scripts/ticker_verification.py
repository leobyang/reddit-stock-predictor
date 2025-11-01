#!/usr/bin/env python3
import csv, os, sys, time
from typing import List, Dict
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

SRC = {
    "nasdaqlisted": "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt",
    "otherlisted":  "https://www.nasdaqtrader.com/dynamic/symdir/otherlisted.txt",
}

def fetch(url: str, retries: int = 3, timeout: int = 30) -> List[Dict[str, str]]:
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            print(f"[fetch] {url} (attempt {attempt}/{retries})")
            req = Request(url, headers={"User-Agent": "curl/8"})
            with urlopen(req, timeout=timeout) as r:
                text = r.read().decode("utf-8", errors="replace").strip().splitlines()
            # Drop footer line "File Creation Time: ..."
            if text and text[-1].startswith("File Creation Time:"):
                text = text[:-1]
            reader = csv.DictReader(text, delimiter="|")
            rows = [row for row in reader]
            if not rows:
                raise ValueError("Empty response or header only.")
            return rows
        except (URLError, HTTPError, TimeoutError, ValueError) as e:
            last_err = e
            print(f"[fetch][warn] {e}")
            time.sleep(1 + attempt)
    raise RuntimeError(f"Failed to fetch {url}: {last_err}")

def normalize_symbol(sym: str) -> str:
    s = (sym or "").strip().upper()
    for sep in (".", "-", "/"):  # trim warrant/unit suffixes
        if sep in s:
            s = s.split(sep, 1)[0]
    return s

def is_equity_issue(row: Dict[str, str]) -> bool:
    if (row.get("ETF","N") or "N").upper() == "Y": return False
    if (row.get("Test Issue","N") or "N").upper() == "Y": return False
    if (row.get("NextShares","N") or "N").upper() == "Y": return False
    return True

def build(out_csv: str):
    print(f"[build] output -> {out_csv}")
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)
    all_rows = []
    for name, url in SRC.items():
        rows = fetch(url)
        print(f"[build] {name}: {len(rows)} raw rows")
        all_rows.extend(rows)

    symbols = set()
    for r in all_rows:
        if not is_equity_issue(r): 
            continue
        sym = normalize_symbol(r.get("Symbol",""))
        if 1 <= len(sym) <= 5 and sym.isalpha():
            symbols.add(sym)

    with open(out_csv, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["symbol"])
        for s in sorted(symbols):
            w.writerow([s])

    print(f"[build] wrote {len(symbols)} symbols to: {out_csv}")

if __name__ == "__main__":
    # default to project root /data/refs/valid_tickers.csv
    default_out = os.path.join(os.path.dirname(__file__), "..", "data", "refs", "valid_tickers.csv")
    out = sys.argv[1] if len(sys.argv) > 1 else default_out
    out = os.path.abspath(out)
    build(out)
