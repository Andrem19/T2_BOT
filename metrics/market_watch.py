#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
market_watch_v2.py
==================
Лёгкий модуль для единовременного сбора:
  • крипто- и финансовых новостей из RSS;
  • ключевых макроэкономических событий (TradingEconomics, опционально investpy).

Назначение: быть встраиваемым компонентом — собрал -> вернул компактные структуры
для последующей отправки в LLM (минимум токенов, чистые поля).

Зависимости: feedparser, requests, python-dotenv (по желанию для TE ключа), (опц.) investpy
"""

from __future__ import annotations

import os
import re
import json
import time
import html
import logging
import hashlib
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional, Tuple

import feedparser
import requests

try:
    import investpy  # опциональный fallback
except Exception:
    investpy = None

# ------------------------- КОНСТАНТЫ И НАСТРОЙКИ ------------------------------

DEFAULT_USER_AGENT = "MarketWatchBot/1.0 (+contact: ops@local) Python/requests feedparser"
NEWS_HORIZON_HOURS_DEFAULT = 24
EVENTS_LOOKAHEAD_DAYS_DEFAULT = 7
MAX_SUMMARY_CHARS_DEFAULT = 260
MAX_ITEMS_PER_FEED_DEFAULT = 40

TE_BASE = "https://api.tradingeconomics.com"

# Базовые фиды (можно переопределить в вызове)
CRYPTO_FEEDS_DEFAULT: Dict[str, str] = {
    "CoinDesk":      "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "CoinTelegraph": "https://cointelegraph.com/rss",
    "Decrypt":       "https://decrypt.co/feed",
}
FINANCE_FEEDS_DEFAULT: Dict[str, str] = {
    "Reuters": "https://feeds.reuters.com/reuters/businessNews",
    "CNBC":    "https://www.cnbc.com/id/100003114/device/rss/rss.html",
    "OilPrice":"https://oilprice.com/rss/main",
}

# ------------------------------ ЛОГГИРОВАНИЕ ----------------------------------

log = logging.getLogger("market_watch_v2")
if not log.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )

# ------------------------------- ДАТАКЛАССЫ -----------------------------------

@dataclass(frozen=True)
class NewsItem:
    category: str           # "crypto" | "finance"
    source: str
    title: str
    url: str
    published_utc: str      # ISO 8601, UTC (YYYY-MM-DDTHH:MM:SSZ)
    summary_short: str      # HTML-stripped, truncated

@dataclass(frozen=True)
class EconEvent:
    name: str
    date_utc: str           # ISO 8601, UTC
    country: Optional[str]
    impact: Optional[str]
    actual: Optional[str]
    previous: Optional[str]
    consensus: Optional[str]
    source: str             # "TradingEconomics" | "Investing.com"

# ------------------------------- УТИЛИТЫ --------------------------------------

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def _to_iso_z(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def _parse_feed_time(entry: dict) -> Optional[datetime]:
    # Берём published_parsed, иначе updated_parsed; если нет — пропускаем.
    if entry.get("published_parsed"):
        tt = entry.published_parsed
    elif entry.get("updated_parsed"):
        tt = entry.updated_parsed
    else:
        return None
    try:
        return datetime(*tt[:6], tzinfo=timezone.utc)
    except Exception:
        return None

_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE  = re.compile(r"\s+")

def _clean_and_truncate(text: str, max_chars: int) -> str:
    if not text:
        return ""
    text = html.unescape(text)
    text = _TAG_RE.sub("", text)
    text = _WS_RE.sub(" ", text).strip()
    if len(text) > max_chars:
        return text[:max_chars].rstrip() + "…"
    return text

def _fingerprint(url: str) -> str:
    return hashlib.md5((url or "").encode("utf-8")).hexdigest()

def _requests_session(user_agent: str = DEFAULT_USER_AGENT, total_retries: int = 3, backoff: float = 0.5) -> requests.Session:
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    sess = requests.Session()
    sess.headers.update({"User-Agent": user_agent, "Accept": "application/json,*/*;q=0.8"})
    retry = Retry(
        total=total_retries,
        backoff_factor=backoff,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "HEAD"])
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    sess.mount("http://", adapter)
    sess.mount("https://", adapter)
    return sess

# ------------------------------- RSS СБОР --------------------------------------

def _parse_rss_feed(feed_url: str, request_headers: Optional[Dict[str, str]] = None):
    # feedparser сам делает HTTP-запрос; подсовываем заголовки для 403-блокеров
    return feedparser.parse(feed_url, request_headers=request_headers or {"User-Agent": DEFAULT_USER_AGENT})

def collect_news(
    crypto_feeds: Optional[Dict[str, str]] = None,
    finance_feeds: Optional[Dict[str, str]] = None,
    horizon_hours: int = NEWS_HORIZON_HOURS_DEFAULT,
    max_items_per_feed: int = MAX_ITEMS_PER_FEED_DEFAULT,
    max_summary_chars: int = MAX_SUMMARY_CHARS_DEFAULT
) -> List[NewsItem]:
    """
    Собирает свежие новости из заданных RSS-источников.
    Возвращает список NewsItem, отсортированный по убыванию времени публикации.
    """
    crypto_feeds = crypto_feeds or CRYPTO_FEEDS_DEFAULT
    finance_feeds = finance_feeds or FINANCE_FEEDS_DEFAULT

    cutoff = utc_now() - timedelta(hours=horizon_hours)
    items: List[NewsItem] = []
    seen: set[str] = set()

    def _ingest(category: str, src: str, url: str):
        nonlocal items, seen
        try:
            feed = _parse_rss_feed(url)
        except Exception as e:
            log.warning("RSS fetch failed [%s/%s]: %s", category, src, e)
            return

        cnt = 0
        for e in feed.entries:
            if cnt >= max_items_per_feed:
                break
            dt = _parse_feed_time(e)
            if not dt or dt < cutoff:
                continue
            title = (e.get("title") or "").strip()
            link = (e.get("link") or "").strip()
            if not title or not link:
                continue
            fp = _fingerprint(link)
            if fp in seen:
                continue
            seen.add(fp)

            summary = (e.get("summary") or e.get("description") or "").strip()
            cleaned = _clean_and_truncate(summary, max_summary_chars)

            items.append(NewsItem(
                category=category,
                source=src,
                title=title,
                url=link,
                published_utc=_to_iso_z(dt),
                summary_short=cleaned
            ))
            cnt += 1

    # crypto
    for src, url in crypto_feeds.items():
        _ingest("crypto", src, url)

    # finance
    for src, url in finance_feeds.items():
        _ingest("finance", src, url)

    # свежие сверху
    items.sort(key=lambda x: x.published_utc, reverse=True)
    return items

# ----------------------- ЭКОНОМИЧЕСКИЙ КАЛЕНДАРЬ -------------------------------

def collect_events_te(
    te_api_key: Optional[str],
    lookahead_days: int = EVENTS_LOOKAHEAD_DAYS_DEFAULT,
    session: Optional[requests.Session] = None,
    timeout_sec: int = 15
) -> List[EconEvent]:
    """
    Получает события TradingEconomics на ближайшие N дней.
    te_api_key: 'guest:guest' приемлемо для демо, лучше личный ключ.
    """
    if not te_api_key:
        raise RuntimeError("TradingEconomics API key is required (set TE_API_KEY).")

    d1 = utc_now().date().isoformat()
    d2 = (utc_now().date() + timedelta(days=lookahead_days)).isoformat()
    url = f"{TE_BASE}/calendar"
    params = {"c": te_api_key, "d1": d1, "d2": d2, "f": "json"}
    sess = session or _requests_session()

    log.debug("TE request %s params=%s", url, params)
    r = sess.get(url, params=params, timeout=timeout_sec)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list):
        log.error("Unexpected TE response: %s", str(data)[:200])
        return []

    out: List[EconEvent] = []
    for it in data:
        # TE обычно отдаёт Date как ISO (UTC). Оставляем как есть,
        # но приводим к Z-формату, если можем распарсить.
        raw_date = it.get("Date")
        dt_iso = None
        if isinstance(raw_date, str):
            s = raw_date.replace("Z", "+00:00")
            try:
                dt = datetime.fromisoformat(s)
                dt_iso = _to_iso_z(dt)
            except Exception:
                dt_iso = raw_date
        out.append(EconEvent(
            name=it.get("Event") or "",
            country=it.get("Country"),
            impact=it.get("Importance"),
            date_utc=dt_iso or "",
            actual=it.get("Actual"),
            previous=it.get("Previous"),
            consensus=it.get("Forecast"),
            source="TradingEconomics"
        ))
    out.sort(key=lambda x: x.date_utc)
    return out

def collect_events_investpy(
    lookahead_days: int = EVENTS_LOOKAHEAD_DAYS_DEFAULT
) -> List[EconEvent]:
    """
    Fallback к Investing.com через investpy ( может ломаться ).
    """
    if investpy is None:
        log.warning("investpy is not installed; skipping investing.com fallback")
        return []
    frm = utc_now().strftime("%d/%m/%Y")
    to  = (utc_now() + timedelta(days=lookahead_days)).strftime("%d/%m/%Y")
    try:
        df = investpy.economic_calendar(from_date=frm, to_date=to, country="all")
    except Exception as e:
        log.warning("investpy.economic_calendar failed: %s", e)
        return []

    out: List[EconEvent] = []
    for _, row in df.iterrows():
        date_txt = f"{row.get('date','')} {row.get('time','')}"
        iso = None
        try:
            dt = datetime.strptime(date_txt, "%d/%m/%Y %H:%M:%S").replace(tzinfo=timezone.utc)
            iso = _to_iso_z(dt)
        except Exception:
            iso = (row.get("date") or "")
        out.append(EconEvent(
            name=row.get("event") or "",
            country=row.get("country"),
            impact=row.get("importance"),
            date_utc=iso,
            actual=row.get("actual"),
            previous=row.get("previous"),
            consensus=row.get("forecast"),
            source="Investing.com"
        ))
    out.sort(key=lambda x: x.date_utc)
    return out

# ------------------------- ПОДГОТОВКА ДЛЯ LLM ---------------------------------

def build_llm_payload(
    news: List[NewsItem],
    events: List[EconEvent],
    max_news: Optional[int] = None,
    max_events: Optional[int] = None
) -> Dict[str, list]:
    """
    Возвращает готовые компактные списки словарей:
      { "news": [...], "events": [...] }
    Порядок уже отсортирован (новости — по убыванию, события — по возрастанию времени).
    """
    if max_news is not None:
        news = news[:max_news]
    if max_events is not None:
        events = events[:max_events]

    return {
        "news": [asdict(n) for n in news],
        "events": [asdict(e) for e in events],
    }

def format_for_llm_text_block(
    news: List[NewsItem],
    events: List[EconEvent],
    max_news: Optional[int] = 40,
    max_events: Optional[int] = 30
) -> str:
    """
    Готовит лаконичный текстовый блок (если хочется отправить «как есть»).
    Лишних токенов не жрёт: по строке на новость/событие.
    """
    if max_news is not None:
        news = news[:max_news]
    if max_events is not None:
        events = events[:max_events]

    lines: List[str] = []
    if news:
        lines.append("NEWS (last 24h):")
        for n in news:
            lines.append(f"- [{n.category}/{n.source}] {n.title} ({n.published_utc}) — {n.summary_short} | {n.url}")
    if events:
        lines.append("EVENTS (next 7d):")
        for e in events:
            imp = f"/{e.impact}" if e.impact else ""
            ap = f" [A:{e.actual} P:{e.previous} C:{e.consensus}]" if (e.actual or e.previous or e.consensus) else ""
            lines.append(f"- [{e.country}{imp}/{e.source}] {e.name} @ {e.date_utc}{ap}")
    return "\n".join(lines)

# ------------------------------ CLI (ОПЦИОНАЛЬНО) -----------------------------

def _cli():
    import argparse
    from dotenv import load_dotenv

    load_dotenv()
    parser = argparse.ArgumentParser(description="Collect market news and macro events (single run).")
    parser.add_argument("--te-key", default=os.getenv("TE_API_KEY", ""), help="TradingEconomics API key (or set TE_API_KEY)")
    parser.add_argument("--horizon-hours", type=int, default=NEWS_HORIZON_HOURS_DEFAULT)
    parser.add_argument("--lookahead-days", type=int, default=EVENTS_LOOKAHEAD_DAYS_DEFAULT)
    parser.add_argument("--max-summary", type=int, default=MAX_SUMMARY_CHARS_DEFAULT)
    parser.add_argument("--max-per-feed", type=int, default=MAX_ITEMS_PER_FEED_DEFAULT)
    parser.add_argument("--max-news", type=int, default=60)
    parser.add_argument("--max-events", type=int, default=40)
    parser.add_argument("--json", action="store_true", help="print JSON payload")
    parser.add_argument("--text", action="store_true", help="print text block for LLM")
    parser.add_argument("--verbose", action="store_true")

    args = parser.parse_args()
    if args.verbose:
        log.setLevel(logging.DEBUG)

    news = collect_news(
        CRYPTO_FEEDS_DEFAULT, FINANCE_FEEDS_DEFAULT,
        horizon_hours=args.horizon_hours,
        max_items_per_feed=args.max_per_feed,
        max_summary_chars=args.max_summary
    )

    sess = _requests_session()
    events = []
    if args.te_key:
        try:
            events = collect_events_te(args.te_key, lookahead_days=args.lookahead_days, session=sess)
        except Exception as e:
            log.warning("TE failed: %s", e)
    if not events:
        # Не критично; можно жить без событий, либо попробовать investpy
        ev_fallback = collect_events_investpy(args.lookahead_days)
        if ev_fallback:
            events = ev_fallback

    payload = build_llm_payload(news, events, max_news=args.max_news, max_events=args.max_events)

    if args.text:
        print(format_for_llm_text_block(news, events, max_news=args.max_news, max_events=args.max_events))
    elif args.json:
        print(json.dumps(payload, ensure_ascii=False))
    else:
        # По умолчанию JSON — удобнее машинно обрабатывать
        print(json.dumps(payload, ensure_ascii=False))

if __name__ == "__main__":
    _cli()
