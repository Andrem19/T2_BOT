# -*- coding: utf-8 -*-
from typing import Optional, List
import time
import math
import random
import logging
import traceback
import requests
import pandas as pd
import plotly.io as pio
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import webbrowser
from pathlib import Path
import shutil


# === ЛОГИРОВАНИЕ ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("stables_plot")


# === НЕ используем системный «браузер по умолчанию» (во избежание запуска Nuclear) ===
# pio.renderers.default оставим пустым — открывать будем сами через webbrowser.get(...)
# pio.renderers.default = "browser"  # <- намеренно НЕ задаём


def _get_browser_controller(preferred: Optional[List[str]] = None):
    """
    Возвращает контроллер webbrowser для одного из известных браузеров.
    Порядок поиска можно задать через preferred. Иначе берём разумный дефолт.
    """
    candidates = preferred or [
        "firefox",
        "google-chrome",
        "chromium-browser",
        "chromium",
        "brave-browser",
        "microsoft-edge",
        "opera"
    ]

    # webbrowser может знать «имена» контроллеров, но надёжнее проверить бинарь через shutil.which
    name_to_exec = {
        "firefox": "firefox",
        "google-chrome": "google-chrome",
        "chromium-browser": "chromium-browser",
        "chromium": "chromium",
        "brave-browser": "brave-browser",
        "microsoft-edge": "microsoft-edge",
        "opera": "opera",
    }

    for name in candidates:
        exe = name_to_exec.get(name)
        if not exe:
            continue
        if shutil.which(exe):
            try:
                ctrl = webbrowser.get(using=name)
                log.info(f"[BROWSER] Используем браузер: {name} ({exe})")
                return ctrl
            except webbrowser.Error:
                # Попробуем явный путь, если имя не зарегистрировано
                path = shutil.which(exe)
                if path:
                    try:
                        ctrl = webbrowser.get(f"{path} %s")
                        log.info(f"[BROWSER] Используем браузер по пути: {path}")
                        return ctrl
                    except webbrowser.Error:
                        pass

    log.warning("[BROWSER] Не найден ни один из явных браузеров. Будем пробовать системный default (xdg-open).")
    try:
        ctrl = webbrowser.get()
        return ctrl
    except webbrowser.Error:
        return None


def fetch_binance_klines_1m(
    symbol: str,
    start_ms: int,
    end_ms: int,
    *,
    market: str = "futures",        # "futures" (USDT-perp) или "spot"
    limit: int = 1500,              # макс. у Binance: 1500
    cooldown_s: float = 0.35,       # пауза между УСПЕШНЫМИ запросами (анти-спам)
    timeout_s: float = 15.0,        # таймаут запроса
    max_retries: int = 6,           # ретраи на батч
    backoff_factor: float = 1.6,    # экспон. бэкофф
    jitter_s: float = 0.25,         # джиттер к паузам
    session: Optional[requests.Session] = None
) -> pd.DataFrame:
    """
    Надёжная пагинация 1m klines с паузами и бэкоффом.
    Возвращает DataFrame: time_open, open, high, low, close, volume, time_close (ms).
    """
    if start_ms > end_ms:
        raise ValueError("start_ms должен быть <= end_ms")

    base_url = (
        "https://fapi.binance.com/fapi/v1/klines" if market == "futures"
        else "https://api.binance.com/api/v3/klines"
    )

    start_ms = (start_ms // 60000) * 60000
    end_ms   = ((end_ms + 59999) // 60000) * 60000 - 1

    owns_session = False
    if session is None:
        session = requests.Session()
        owns_session = True
    session.headers.update({"User-Agent": "klines-loader/1.1 (safe, rate-limited)"})

    out: List[List] = []
    cur = start_ms
    last_request_ts = 0.0

    log.info(f"[KLINES] Начинаем загрузку {symbol} 1m: {start_ms}..{end_ms} (мс)")
    try:
        while cur <= end_ms:
            params = {"symbol": symbol, "interval": "1m", "limit": limit, "startTime": cur}
            attempt = 0
            while True:
                since_last = time.monotonic() - last_request_ts
                if since_last < cooldown_s:
                    sleep_s = cooldown_s - since_last
                    if sleep_s > 0:
                        time.sleep(sleep_s)
                try:
                    resp = session.get(base_url, params=params, timeout=timeout_s)
                    last_request_ts = time.monotonic()

                    if resp.status_code in (418,):
                        log.warning("[KLINES] 418 (ban). Спим и завершаем.")
                        time.sleep(10.0 + random.random() * 2.0)
                        resp.raise_for_status()

                    if resp.status_code == 429:
                        retry_after = resp.headers.get("Retry-After")
                        if retry_after:
                            try:
                                sleep_s = float(retry_after)
                            except ValueError:
                                sleep_s = 2.0
                        else:
                            sleep_s = (backoff_factor ** attempt) + random.uniform(0.0, jitter_s)
                        attempt += 1
                        log.warning(f"[KLINES] 429 Too Many Requests. Sleep={sleep_s:.2f}s (attempt={attempt}/{max_retries})")
                        time.sleep(max(1.0, sleep_s))
                        if attempt > max_retries:
                            resp.raise_for_status()
                        continue

                    resp.raise_for_status()
                    batch = resp.json()
                    break

                except (requests.Timeout, requests.ConnectionError) as e:
                    attempt += 1
                    if attempt > max_retries:
                        log.error("[KLINES] Превышен лимит ретраев (сеть/таймаут).")
                        raise
                    sleep_s = (backoff_factor ** (attempt - 1)) + random.uniform(0.0, jitter_s)
                    log.warning(f"[KLINES] Сетевая ошибка/таймаут: {e}. Retry in {sleep_s:.2f}s (attempt={attempt}/{max_retries})")
                    time.sleep(sleep_s)

                except requests.HTTPError as e:
                    attempt += 1
                    if attempt > max_retries:
                        log.error(f"[KLINES] HTTPError: {e}. Превышен лимит ретраев.")
                        raise
                    sleep_s = (backoff_factor ** (attempt - 1)) + random.uniform(0.0, jitter_s)
                    log.warning(f"[KLINES] HTTPError: {e}. Retry in {sleep_s:.2f}s (attempt={attempt}/{max_retries})")
                    time.sleep(sleep_s)

            if not batch:
                log.info("[KLINES] Пустой батч — завершаем.")
                break

            out_len_before = len(out)
            out.extend(batch)
            out_added = len(out) - out_len_before
            last_open = int(batch[-1][0])
            next_cur = last_open + 60_000
            if next_cur <= cur:
                next_cur = cur + 60_000

            log.info(f"[KLINES] +{out_added} свечей. last_open={last_open}, next_cur={next_cur}")
            cur = next_cur

            if cur > end_ms:
                log.info("[KLINES] Дошли до конца диапазона.")
                break

            time.sleep(cooldown_s + random.uniform(0.0, jitter_s))

        if not out:
            log.warning("[KLINES] В итоге 0 свечей.")
            return pd.DataFrame(columns=["time_open", "open", "high", "low", "close", "volume", "time_close"])

        rows = []
        for k in out:
            ot = int(k[0])
            if ot < start_ms or ot > end_ms:
                continue
            rows.append([ot, float(k[1]), float(k[2]), float(k[3]), float(k[4]), float(k[5]), int(k[6])])

        df = pd.DataFrame(rows, columns=["time_open", "open", "high", "low", "close", "volume", "time_close"])
        if not df.empty:
            df = df.sort_values("time_open").drop_duplicates(subset=["time_open"], keep="last").reset_index(drop=True)

        log.info(f"[KLINES] Готово: {len(df)} свечей.")
        return df

    finally:
        if owns_session:
            try:
                session.close()
            except Exception:
                pass


def plot_stables_with_btc(csv_path: str,
                          *,
                          tz: str = "UTC",
                          symbol: str = "BTCUSDT",
                          width: int = 2600,
                          height: int = 720,
                          line_width: float = 0.7,
                          save_html: str = "stables_with_btc.html",
                          open_in_browser: bool = True,
                          preferred_browsers: Optional[List[str]] = None) -> go.Figure:
    """
    Читает stables_metrics.csv, строит две линии (stable_score/confidence, WebGL) и 1m свечи BTCUSDT.
    Общая ось X, «умные» метки времени (минуты при зуме), rangeslider снизу.
    HTML сохраняется на диск (CDN-скрипт). По желанию — открываем в явном браузере.
    """
    try:
        log.info(f"[CSV] Читаем файл: {csv_path}")
        df = pd.read_csv(csv_path, usecols=["time_utc", "stable_score", "confidence"])
        log.info(f"[CSV] Загружено строк: {len(df)}")

        df["time_utc"] = pd.to_numeric(df["time_utc"], errors="coerce")
        df["stable_score"] = pd.to_numeric(df["stable_score"], errors="coerce")
        df["confidence"]   = pd.to_numeric(df["confidence"], errors="coerce")
        df = df.dropna(subset=["time_utc", "stable_score", "confidence"]).reset_index(drop=True)
        log.info(f"[CSV] После очистки строк: {len(df)}")

        if df.empty:
            raise ValueError("stables_metrics.csv не содержит валидных данных.")

        start_ms = int(math.floor(df["time_utc"].min() * 1000))
        end_ms   = int(math.ceil(df["time_utc"].max() * 1000))
        log.info(f"[CSV] Диапазон (мс): start={start_ms}, end={end_ms}")

        # Время: datetime(UTC) -> при необходимости tz -> делаем naive
        t = pd.to_datetime(df["time_utc"], unit="s", utc=True)
        if tz and tz.upper() != "UTC":
            t = t.dt.tz_convert(tz)
        t = t.dt.tz_localize(None)

        # Свечи
        try:
            kl_df = fetch_binance_klines_1m(symbol, start_ms, end_ms + 59_999, market="futures")
            log.info(f"[KLINES] Загружено свечей: {len(kl_df)}")
        except Exception as e:
            log.error("[KLINES] Ошибка загрузки свечей:")
            log.error(traceback.format_exc())
            kl_df = pd.DataFrame(columns=["time_open", "open", "high", "low", "close", "volume", "time_close"])

        if not kl_df.empty:
            ktime = pd.to_datetime(kl_df["time_open"], unit="ms", utc=True)
            if tz and tz.upper() != "UTC":
                ktime = ktime.dt.tz_convert(tz)
            ktime = ktime.dt.tz_localize(None)
        else:
            ktime = pd.Series([], dtype="datetime64[ns]")

        # Фигура
        fig = make_subplots(
            rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.06,
            row_heights=[0.55, 0.45],
            specs=[[{"secondary_y": True}], [{"type": "candlestick"}]]
        )

        # Линии (WebGL)
        fig.add_trace(go.Scattergl(x=t, y=df["stable_score"], mode="lines",
                                   name="stable_score", line=dict(width=line_width)),
                      row=1, col=1, secondary_y=False)
        fig.add_trace(go.Scattergl(x=t, y=df["confidence"], mode="lines",
                                   name="confidence", line=dict(width=line_width)),
                      row=1, col=1, secondary_y=True)

        # Свечи
        if not kl_df.empty:
            fig.add_trace(go.Candlestick(x=ktime, open=kl_df["open"], high=kl_df["high"],
                                         low=kl_df["low"], close=kl_df["close"], name=f"{symbol} 1m"),
                          row=2, col=1)
        else:
            log.warning("[KLINES] Свечей нет — нижняя панель пустая.")

        # «Умные» метки времени
        tickformatstops = [
            dict(dtickrange=[None, 1000 * 60 * 60], value="%H:%M"),        # < 1 часа — минуты
            dict(dtickrange=[1000 * 60 * 60, 1000 * 60 * 60 * 24], value="%H:%M"),
            dict(dtickrange=[1000 * 60 * 60 * 24, None], value="%d %b %H:%M")
        ]
        fig.update_xaxes(tickformat="%H:%M", tickformatstops=tickformatstops,
                         showgrid=True, gridcolor="rgba(0,0,0,0.15)", griddash="solid",
                         row=1, col=1)
        fig.update_xaxes(tickformat="%H:%M", tickformatstops=tickformatstops,
                         showgrid=True, gridcolor="rgba(0,0,0,0.15)", griddash="solid",
                         rangeslider=dict(visible=True, thickness=0.08),
                         row=2, col=1)

        # Оси Y
        fig.update_yaxes(title_text="stable_score", showgrid=True, gridcolor="rgba(0,0,0,0.12)",
                         row=1, col=1, secondary_y=False)
        fig.update_yaxes(title_text="confidence", showgrid=False, row=1, col=1, secondary_y=True)
        fig.update_yaxes(title_text=f"{symbol} price", showgrid=True, gridcolor="rgba(0,0,0,0.12)",
                         row=2, col=1)

        # Общие настройки и конфиг
        x_tz = "UTC" if (not tz or tz.upper() == "UTC") else tz
        fig.update_layout(
            width=width, height=height,
            margin=dict(l=70, r=70, t=35, b=45),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
            xaxis_title=f"Time ({x_tz})"
        )
        fig.update_traces(selector=dict(type="scattergl"),
                          hovertemplate="%{x|%H:%M}<br>%{y:.6f}")
        config = dict(scrollZoom=True, displaylogo=False)

        # Сохранение HTML
        html_path = Path(save_html).resolve()
        fig.write_html(str(html_path), include_plotlyjs="cdn", config=config)
        log.info(f"[PLOT] HTML сохранён: {html_path}")

        # Открытие в явном браузере (минуя системный default)
        if open_in_browser:
            ctrl = _get_browser_controller(preferred=preferred_browsers)
            if ctrl is not None:
                opened = ctrl.open(f"file://{html_path}", new=2)  # new=2 — вкладка
                log.info(f"[BROWSER] Попытка открытия в явном браузере: {'OK' if opened else 'неизвестно'}")
            else:
                log.warning("[BROWSER] Не удалось получить контроллер браузера. Откройте файл вручную.")

        return fig

    except Exception:
        log.error("[MAIN] Ошибка при построении графика:")
        log.error(traceback.format_exc())
        raise


if __name__ == "__main__":
    # Пример: явно просим Firefox, потом Chrome, потом Chromium...
    plot_stables_with_btc(
        "stables_metrics.csv",
        tz="UTC",
        symbol="BTCUSDT",
        preferred_browsers=["brave-browser", "firefox", "google-chrome", "chromium-browser", "chromium"]
    )
