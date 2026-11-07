#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
雪球 全市场 × 指定结束日往前 N 个交易日 的主力资金流 / 价格 / 涨幅 → U:\Grab\Tip.db

默认：以 2026-11-06 为最后一个交易日，往前 20 个交易日，全市场 A 股。

交易日历取自上证指数（SH000001）的日线，保证所有股票用同一套日期；
每只股票取 1 次 capital/history.json（主力净额）+ 1 次 chart/kline.json（收盘价、涨跌幅）。

写入表 IndividualFundFlow_XQ（6 列）：
    TradeDate, StockCode, StockName, Price, Rise, FundFlow_MainForce
金额单位：万元，保留 1 位小数。
"""

from __future__ import annotations

import argparse
import logging
import os
import random
import re
import sqlite3
import sys
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

import requests

STOCK_BASE = "https://stock.xueqiu.com"
HOME = "https://xueqiu.com/"
TZ = timezone(timedelta(hours=8))
SYMBOL_RE = re.compile(r"^(SH|SZ|BJ)\d{6}$")

DEFAULT_DB = r"U:\Grab\Tip.db"
TABLE = "IndividualFundFlow_XQ"
WAN = 10000.0
DEFAULT_END = "2026-11-06"
DEFAULT_DAYS = 20

DDL = f"""
CREATE TABLE IF NOT EXISTS "{TABLE}" (
  "TradeDate" DATE NOT NULL,
  "StockCode" TEXT NOT NULL,
  "StockName" TEXT,
  "Price" real,
  "Rise" real,
  "FundFlow_MainForce" real,
  PRIMARY KEY ("TradeDate", "StockCode")
);
CREATE INDEX IF NOT EXISTS "idx_iff_xq_code" ON "{TABLE}" ("StockCode");
"""

INSERT_SQL = f"""
INSERT OR REPLACE INTO "{TABLE}"
("TradeDate","StockCode","StockName","Price","Rise","FundFlow_MainForce")
VALUES (?,?,?,?,?,?)
"""

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Referer": HOME,
    "Origin": HOME,
    "Connection": "keep-alive",
}

log = logging.getLogger("xq")


class AuthError(RuntimeError):
    """Cookie 失效（400016）。"""


class RateLimiter:
    def __init__(self, rps: float) -> None:
        self.interval = 1.0 / max(rps, 0.1)
        self._lock = threading.Lock()
        self._next = 0.0

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            start = max(now, self._next)
            self._next = start + self.interval
            delay = start - now
        if delay > 0:
            time.sleep(delay)


class Xueqiu:
    def __init__(self, rps: float = 5.0, timeout: int = 15) -> None:
        self.timeout = timeout
        self.limiter = RateLimiter(rps)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

        cookie = os.environ.get("XQ_COOKIE", "").strip()
        if cookie:
            for part in cookie.split(";"):
                if "=" in part:
                    key, value = part.strip().split("=", 1)
                    self.session.cookies.set(key, value, domain=".xueqiu.com")
            log.info("使用环境变量 XQ_COOKIE 中的 Cookie")
        else:
            try:
                self.session.get(HOME, timeout=timeout)
            except requests.RequestException as exc:
                log.warning("访问主页失败：%s", exc)
            if "xq_a_token" not in self.session.cookies.get_dict():
                log.warning("没有拿到 xq_a_token，请设置 XQ_COOKIE，否则会返回 400016")

    def get(self, path: str, **params: Any) -> Any:
        url = STOCK_BASE + path
        last_exc: Exception | None = None
        for attempt in range(4):
            self.limiter.wait()
            try:
                resp = self.session.get(url, params=params, timeout=self.timeout)
            except requests.RequestException as exc:
                last_exc = exc
                time.sleep(1.5 * (attempt + 1))
                continue
            if resp.status_code == 429 or resp.status_code >= 500:
                last_exc = RuntimeError(f"HTTP {resp.status_code}")
                time.sleep(2.0 * (attempt + 1) + random.random())
                continue
            resp.raise_for_status()
            payload = resp.json()
            code = payload.get("error_code")
            if code in (0, "0", None):
                return payload.get("data")
            if str(code) == "400016":
                raise AuthError("400016：Cookie 无效或过期，请重新复制 XQ_COOKIE。")
            last_exc = RuntimeError(f"{code}: {payload.get('error_description')}")
            time.sleep(1.0 * (attempt + 1))
        raise RuntimeError(f"请求失败 {path} {params}: {last_exc}")


def day_of(ms: Any) -> str | None:
    if not isinstance(ms, (int, float)) or ms <= 0:
        return None
    return datetime.fromtimestamp(ms / 1000, TZ).date().isoformat()


def num(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def wan(value: Any) -> float | None:
    amount = num(value)
    return None if amount is None else round(amount / WAN, 1)


def as_items(payload: Any) -> list:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("items", "list", "records"):
            if isinstance(payload.get(key), list):
                return payload[key]
    return []


def end_ms(date: str) -> int:
    """该日期当天 23:59:59 的毫秒时间戳，作为 kline 的 begin 上界。"""
    dt = datetime.fromisoformat(date).replace(hour=23, minute=59, second=59, tzinfo=TZ)
    return int(dt.timestamp() * 1000)


def trading_calendar(xq: Xueqiu, end: str, days: int) -> list[str]:
    """用上证指数日线确定「end 及之前」的最近 days 个交易日。"""
    data = xq.get(
        "/v5/stock/chart/kline.json",
        symbol="SH000001", begin=end_ms(end), period="day",
        type="normal", count=-(days + 15), indicator="kline",
    ) or {}
    cols = data.get("column") or []
    rows = data.get("item") or []
    if "timestamp" not in cols:
        raise RuntimeError("取交易日历失败：返回里没有 timestamp 列")
    i = cols.index("timestamp")
    dates = [d for d in (day_of(r[i]) for r in rows) if d and d <= end]
    dates = sorted(set(dates))
    if len(dates) < days:
        log.warning("指数只返回 %d 个交易日，不足 %d 天", len(dates), days)
    return dates[-days:]


def stock_kline(xq: Xueqiu, symbol: str, end: str) -> dict[str, tuple[float | None, float | None]]:
    """{日期: (收盘价, 涨跌幅%)}，前复权。"""
    data = xq.get(
        "/v5/stock/chart/kline.json",
        symbol=symbol, begin=end_ms(end), period="day",
        type="before", count=-40, indicator="kline",
    ) or {}
    cols = data.get("column") or []
    rows = data.get("item") or []
    if not cols or not rows:
        return {}
    idx = {c: i for i, c in enumerate(cols)}
    if "timestamp" not in idx:
        return {}
    out: dict[str, tuple[float | None, float | None]] = {}
    for row in rows:
        day = day_of(row[idx["timestamp"]])
        if day:
            out[day] = (
                num(row[idx["close"]]) if "close" in idx else None,
                num(row[idx["percent"]]) if "percent" in idx else None,
            )
    return out


def stock_flow(xq: Xueqiu, symbol: str) -> dict[str, float | None]:
    """{日期: 主力净流入(万元)}。雪球只给最近约 20 个交易日。"""
    data = xq.get("/v5/stock/capital/history.json", symbol=symbol, count=40) or {}
    out: dict[str, float | None] = {}
    for item in as_items(data):
        if not isinstance(item, dict):
            continue
        day = day_of(item.get("timestamp"))
        if day:
            out[day] = wan(item.get("amount"))
    return out


def collect_symbols(xq: Xueqiu) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for exchange in ("sh_sz", "bj"):
        page = 1
        while True:
            data = xq.get(
                "/v5/stock/screener/screen.json",
                category="CN", exchange=exchange, page=page, size=100,
                order="desc", order_by="mc", only_count=0,
            ) or {}
            rows = as_items(data)
            if not rows:
                break
            for row in rows:
                symbol = str(row.get("symbol") or "").upper()
                if SYMBOL_RE.match(symbol) and symbol not in seen:
                    seen.add(symbol)
                    out.append({"symbol": symbol, "name": row.get("name")})
            total = int(data.get("count") or 0) if isinstance(data, dict) else 0
            log.info("列表 %s 第 %d 页，累计 %d 只", exchange, page, len(out))
            if (total and page * 100 >= total) or len(rows) < 100:
                break
            page += 1
    return out


def fetch_stock(xq: Xueqiu, stock: dict[str, Any], dates: list[str], end: str) -> tuple[list[tuple], str]:
    """返回该股票在窗口内的所有行，以及状态。"""
    symbol = stock["symbol"]
    try:
        flow = stock_flow(xq, symbol)
        kline = stock_kline(xq, symbol, end)
    except AuthError:
        raise
    except Exception:
        return [], "request"

    rows: list[tuple] = []
    for day in dates:
        price, rise = kline.get(day, (None, None))
        main = flow.get(day)
        if price is None and main is None:
            continue          # 停牌或无数据，跳过这一天
        rows.append((day, symbol, stock.get("name"), price, rise, main))

    if not rows:
        return [], "no_data"
    return rows, "ok"


def open_db(path: str, init: bool) -> sqlite3.Connection:
    if not os.path.isfile(path) and not init:
        raise SystemExit(f"数据库文件不存在：{path}（首次请加 --init）")
    con = sqlite3.connect(path, timeout=30)
    if init:
        con.executescript(DDL)
        con.commit()
        log.info("已确保表 %s 存在（6 列）", TABLE)
    else:
        if not con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (TABLE,)
        ).fetchone():
            con.close()
            raise SystemExit(f"数据库 {path} 中没有表 {TABLE}，请先加 --init。")
    return con


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="雪球 全市场 × N 个交易日 的主力资金流/价格/涨幅")
    parser.add_argument("--db", default=DEFAULT_DB, help=f"库文件，默认 {DEFAULT_DB}")
    parser.add_argument("--end", default=DEFAULT_END, help=f"最后一个交易日，默认 {DEFAULT_END}")
    parser.add_argument("--days", type=int, default=DEFAULT_DAYS, help=f"往前多少个交易日，默认 {DEFAULT_DAYS}")
    parser.add_argument("--init", action="store_true", help="建表（首次）")
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--rps", type=float, default=5.0)
    parser.add_argument("--limit", type=int, default=0, help="只处理前 N 只（调试）")
    parser.add_argument("--print-rows", action="store_true", help="逐行打印所有入库数据")
    parser.add_argument("--dry-run", action="store_true", help="只抓取不写库")
    parser.add_argument("--batch", type=int, default=500, help="每多少只股票提交一次")
    args = parser.parse_args(list(argv) if argv is not None else None)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")

    con = open_db(args.db, args.init)
    log.info("目标数据库：%s", os.path.abspath(args.db))

    xq = Xueqiu(rps=args.rps)
    started = time.time()

    dates = trading_calendar(xq, args.end, args.days)
    if not dates:
        log.error("没取到交易日历")
        con.close()
        return 1
    log.info("交易日窗口：%s ~ %s（共 %d 天）", dates[0], dates[-1], len(dates))

    stocks = collect_symbols(xq)
    if not stocks:
        log.error("没拿到股票列表")
        con.close()
        return 1
    if args.limit:
        stocks = stocks[: args.limit]
    log.info("待抓取 %d 只，预计 %d 次请求，约 %.0f 分钟",
             len(stocks), len(stocks) * 2 + 60, (len(stocks) * 2 + 60) / args.rps / 60)

    buf: list[tuple] = []
    reasons: Counter = Counter()
    committed = 0
    stocks_done = 0

    def flush() -> None:
        nonlocal committed
        if not buf:
            return
        if args.print_rows:
            for row in buf:
                log.info("  %s  %-8s %-8s 价=%s 涨=%s 主力=%s万",
                         row[0], row[1], str(row[2] or "")[:8], row[3], row[4], row[5])
        if args.dry_run:
            committed += len(buf)
        else:
            con.executemany(INSERT_SQL, buf)
            con.commit()
            committed += len(buf)
        log.info("提交 %d 行（累计 %d 行，%d 只股票完成）", len(buf), committed, stocks_done)
        buf.clear()

    try:
        with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
            futures = {pool.submit(fetch_stock, xq, s, dates, args.end): s for s in stocks}
            for i, fut in enumerate(as_completed(futures), 1):
                try:
                    rows, reason = fut.result()
                except AuthError as exc:
                    for f in futures:
                        f.cancel()
                    flush()
                    log.error("%s", exc)
                    return 2
                except Exception as exc:
                    rows, reason = [], f"exc:{type(exc).__name__}"
                reasons[reason] += 1
                if rows:
                    buf.extend(rows)
                    stocks_done += 1
                if i % 200 == 0 or len(buf) >= args.batch * 20 or i == len(stocks):
                    flush()
                    log.info("进度 %d/%d（有效股票 %d）%s", i, len(stocks), stocks_done, dict(reasons))
        flush()
    finally:
        con.close()

    log.info("完成：%d 只股票、%d 行，耗时 %.1f 分钟，原因统计 %s",
             stocks_done, committed, (time.time() - started) / 60, dict(reasons))

    if not args.dry_run:
        check = sqlite3.connect(args.db, timeout=30)
        got, = check.execute(
            f'SELECT COUNT(*) FROM "{TABLE}" WHERE "TradeDate" BETWEEN ? AND ?',
            (dates[0], dates[-1]),
        ).fetchone()
        check.close()
        log.info("回读确认：%s ~ %s 区间表内共 %d 行", dates[0], dates[-1], got)
    return 0


if __name__ == "__main__":
    sys.exit(main())