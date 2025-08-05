# LIQ_ALGO/data_loader.py
import helpers.tools as tools
import asyncio
from typing import Dict, Any
import numpy as np
import datetime
from typing import Optional, Union


def load_candles(
    path: str,
    start_date: Optional[Union[str, datetime.date, datetime.datetime]] = None,
    end_date:   Optional[Union[str, datetime.date, datetime.datetime]] = None,
    chunk_size: int = 100_000
) -> np.ndarray:
    """
    Загружает свечные данные [timestamp_ms, open, high, low, close, volume] из CSV,
    отфильтрованные по датам в формате ДД-ММ-ГГГГ (или ДД.ММ.ГГГГ, ДД/ММ/ГГГГ).

    :param path:       путь к CSV (строки: ms, open, high, low, close, volume).
    :param start_date: начало (включительно) — строка 'DD-MM-YYYY'/'DD.MM.YYYY'/...
                       или datetime.date/datetime.datetime.
    :param end_date:   конец   (включительно) — аналогично.
    :param chunk_size: сколько строк читать за раз.
    :return:           ndarray формы (N,6) с данными между start_date и end_date.
    """

    def to_ts_ms(dt):
        if dt is None:
            return None
        # Если передали строку, ожидаем 'DD-MM-YYYY' или варианты с '.', '/', ' '
        if isinstance(dt, str):
            for fmt in ("%d-%m-%Y", "%d.%m.%Y", "%d/%m/%Y", "%d %m %Y"):
                try:
                    dt = datetime.datetime.strptime(dt, fmt)
                    break
                except ValueError:
                    continue
            else:
                raise ValueError(f"Unrecognized date format: {dt!r}. "
                                 "Use 'DD-MM-YYYY', 'DD.MM.YYYY', 'DD/MM/YYYY' or 'DD MM YYYY'.")
        # Если передали date без времени — приравниваем к началу дня
        if isinstance(dt, datetime.date) and not isinstance(dt, datetime.datetime):
            dt = datetime.datetime.combine(dt, datetime.time.min)
        # Если datetime — берём его timestamp
        if isinstance(dt, datetime.datetime):
            return int(dt.timestamp() * 1000)
        raise TypeError(f"start_date/end_date must be str or date/datetime, got {type(dt)}")

    start_ts = to_ts_ms(start_date)
    end_ts   = to_ts_ms(end_date)

    out_chunks = []
    with open(path, 'r') as f:
        # пропускаем возможный заголовок
        first = f.readline()
        try:
            vals = np.fromstring(first, sep=',')
            if len(vals) < 6:
                # это заголовок — продолжаем после него
                pass
            else:
                # это данные — возвращаемся к началу файла
                f.seek(0)
        except:
            # явно заголовок
            pass

        # читаем файл по частям
        while True:
            chunk = np.genfromtxt(f, delimiter=',', max_rows=chunk_size)
            if chunk.size == 0:
                break
            # единичная строка → приводим к 2D
            if chunk.ndim == 1:
                chunk = chunk.reshape(1, -1)

            # фильтруем до начала
            if start_ts is not None:
                chunk = chunk[chunk[:, 0] >= start_ts]
            # фильтруем после конца
            if end_ts is not None:
                mask = chunk[:, 0] <= end_ts
                chunk = chunk[mask]
                # Если в этом куске встретили точку конца — добавляем и выходим
                if chunk.size and chunk[-1, 0] >= end_ts:
                    out_chunks.append(chunk)
                    break

            if chunk.size:
                out_chunks.append(chunk)

    if not out_chunks:
        return np.empty((0, 6))
    return np.vstack(out_chunks)


def build_weekly_volatility_dict(
    path: str,
    start_date: str,
    end_date: str,
    window_size: int = 7
) -> Dict[int, float]:
    """
    Load candle data from CSV and compute percentage volatility over the past `window_size`
    periods for each timestamp.

    Returns a dict mapping timestamp (ms) -> volatility (%).
    """
    # Load data as a NumPy array: each row = [timestamp, open, high, low, close, ...]
    data = load_candles(
        path=path,
        start_date=start_date,
        end_date=end_date
    )

    vol_dict: Dict[int, float] = {}

    for i in range(window_size, len(data)):
        # slice out the previous `window_size` rows
        window = data[i - window_size : i]

        # assume column indices: 2 = high, 3 = low
        lows  = window[:, 3]
        highs = window[:, 2]

        min_price = float(lows.min())
        max_price = float(highs.max())

        # calculate percent difference between extrema
        perc_vol = tools.calculate_percent_difference(min_price, max_price)

        # timestamp in ms as int
        ts = int(data[i][0])
        vol_dict[ts] = perc_vol

    return vol_dict