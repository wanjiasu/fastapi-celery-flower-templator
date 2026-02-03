import argparse
import json
import os
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

import psycopg2
from psycopg2.extras import execute_values

TUSHARE_API_URL = "https://api.tushare.pro"

DEFAULT_FIELDS = [
    "ts_code",
    "symbol",
    "name",
    "area",
    "industry",
    "fullname",
    "enname",
    "cnspell",
    "market",
    "exchange",
    "curr_type",
    "list_status",
    "list_date",
    "delist_date",
    "is_hs",
    "act_name",
    "act_ent_type",
]

_dotenv_cache: dict[str, str] | None = None


def _load_dotenv() -> dict[str, str]:
    global _dotenv_cache
    if _dotenv_cache is not None:
        return _dotenv_cache

    env_path = Path(__file__).resolve().parents[2] / ".env"
    values: dict[str, str] = {}
    if env_path.exists():
        for raw_line in env_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key:
                values[key] = value

    _dotenv_cache = values
    return values


def _get_env(key: str) -> str | None:
    value = os.getenv(key)
    if value:
        return value
    return _load_dotenv().get(key)


def _require_first_env(*keys: str) -> str:
    for key in keys:
        value = _get_env(key)
        if value:
            return value
    raise RuntimeError(f"missing required env: {', '.join(keys)}")


def _parse_yyyymmdd(value: Any):
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.strptime(text, "%Y%m%d").date()
    except ValueError:
        return None


def fetch_stock_basic(
    token: str,
    *,
    exchange: str = "",
    list_status: str = "L",
    fields: list[str] | None = None,
    timeout_s: int = 30,
) -> list[dict[str, Any]]:
    use_fields = fields or DEFAULT_FIELDS
    payload = {
        "api_name": "stock_basic",
        "token": token,
        "params": {"exchange": exchange, "list_status": list_status},
        "fields": ",".join(use_fields),
    }

    req = urllib.request.Request(
        TUSHARE_API_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            raw = resp.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"tushare http error: {e.code}") from e
    except urllib.error.URLError as e:
        raise RuntimeError("tushare request failed") from e

    body = json.loads(raw)
    if body.get("code") != 0:
        raise RuntimeError(f"tushare error: {body.get('msg')}")

    data = body.get("data") or {}
    resp_fields: list[str] = data.get("fields") or []
    items: list[list[Any]] = data.get("items") or []
    return [dict(zip(resp_fields, item, strict=False)) for item in items]


def ensure_stock_basic_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS stock_basic (
                ts_code TEXT PRIMARY KEY,
                symbol TEXT,
                name TEXT,
                area TEXT,
                industry TEXT,
                fullname TEXT,
                enname TEXT,
                cnspell TEXT,
                market TEXT,
                exchange TEXT,
                curr_type TEXT,
                list_status TEXT,
                list_date DATE,
                delist_date DATE,
                is_hs TEXT,
                act_name TEXT,
                act_ent_type TEXT,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            """
        )


def upsert_stock_basic(conn, rows: Iterable[dict[str, Any]]) -> int:
    columns = [
        "ts_code",
        "symbol",
        "name",
        "area",
        "industry",
        "fullname",
        "enname",
        "cnspell",
        "market",
        "exchange",
        "curr_type",
        "list_status",
        "list_date",
        "delist_date",
        "is_hs",
        "act_name",
        "act_ent_type",
    ]

    values = []
    for row in rows:
        values.append(
            (
                row.get("ts_code"),
                row.get("symbol"),
                row.get("name"),
                row.get("area"),
                row.get("industry"),
                row.get("fullname"),
                row.get("enname"),
                row.get("cnspell"),
                row.get("market"),
                row.get("exchange"),
                row.get("curr_type"),
                row.get("list_status"),
                _parse_yyyymmdd(row.get("list_date")),
                _parse_yyyymmdd(row.get("delist_date")),
                row.get("is_hs"),
                row.get("act_name"),
                row.get("act_ent_type"),
            )
        )

    if not values:
        return 0

    set_columns = [c for c in columns if c != "ts_code"]
    set_clause = ", ".join([f"{c} = EXCLUDED.{c}" for c in set_columns] + ["updated_at = now()"])
    insert_sql = f"""
        INSERT INTO stock_basic ({", ".join(columns)})
        VALUES %s
        ON CONFLICT (ts_code)
        DO UPDATE SET {set_clause};
    """

    with conn.cursor() as cur:
        execute_values(cur, insert_sql, values, page_size=2000)

    return len(values)


def sync_stock_basic_to_postgres(
    *,
    exchange: str = "",
    list_status: str = "L",
    fields: list[str] | None = None,
    timeout_s: int = 30,
) -> dict[str, int]:
    token = _require_first_env("TUSHARE_TOKEN", "TUSHARE_PRO_TOKEN")
    dsn = _require_first_env("POSTGRES_DSN")

    rows = fetch_stock_basic(
        token,
        exchange=exchange,
        list_status=list_status,
        fields=fields,
        timeout_s=timeout_s,
    )

    with psycopg2.connect(dsn) as conn:
        ensure_stock_basic_table(conn)
        saved = upsert_stock_basic(conn, rows)

    return {"fetched": len(rows), "saved": saved}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--exchange", default="")
    parser.add_argument("--list-status", default="L")
    parser.add_argument("--timeout-s", type=int, default=30)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    token = _require_first_env("TUSHARE_TOKEN", "TUSHARE_PRO_TOKEN")
    rows = fetch_stock_basic(
        token,
        exchange=args.exchange,
        list_status=args.list_status,
        timeout_s=args.timeout_s,
    )

    if args.dry_run:
        print(json.dumps({"fetched": len(rows)}, ensure_ascii=False))
        return

    dsn = _require_first_env("POSTGRES_DSN")
    with psycopg2.connect(dsn) as conn:
        ensure_stock_basic_table(conn)
        saved = upsert_stock_basic(conn, rows)

    print(json.dumps({"fetched": len(rows), "saved": saved}, ensure_ascii=False))


if __name__ == "__main__":
    main()
