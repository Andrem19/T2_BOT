from datetime import datetime, timezone, timedelta, date
from typing import Any, Dict, Tuple, List, Literal, Optional
import math
import bisect

def calc_futures_qty(
    upper_price: float,
    lower_price: float,
    current_price: float,
    opt_abs_upper: float,
    opt_abs_lower: float,
    option_type: str = "put",
    share_target: float = 0.7,
    contract_multiplier: float = 1.0,
) -> dict:
    # print(upper_price, lower_price, current_price, opt_abs_upper, opt_abs_lower, option_type)
    """
    Рассчитать размер позиции по фьючерсу, обеспечивающей положительный итоговый PnL 
    портфеля (опцион + фьючерс) как на верхней, так и на нижней контрольных ценах,
    причём на доминирующей стороне должно приходиться `share_target` от суммарного
    результата, а на противоположной — `1 − share_target`.

    Доминирующей стороной считается та, где фьючерс даёт плюс:
        • для put-опциона это ВЕРХНЯЯ граница → потребуется LONG-фьючерс;
        • для call-опциона это НИЖНЯЯ граница → потребуется SHORT-фьючерс.

    Параметры
    ----------
    upper_price : float
        Верхняя контрольная цена.
    lower_price : float
        Нижняя контрольная цена.
    current_price : float
        Текущая цена базового актива.
    opt_abs_upper : float
        Абсолютная величина PnL опциона на верхней границе (положительная).
        Для put — это УБЫТОК, для call — ПРИБЫЛЬ.
    opt_abs_lower : float
        Абсолютная величина PnL опциона на нижней границе (положительная).
        Для put — это ПРИБЫЛЬ, для call — УБЫТОК.
    option_type : {'put', 'call'}, default 'put'
        Тип опциона.
    share_target : float, default 0.7
        Доля совокупного PnL, которую хотим получить на доминирующей стороне
        (0 < share_target < 1). На другой стороне будет 1 − share_target.
    contract_multiplier : float, default 1.0
        Мультипликатор фьючерсного контракта.

    Возвращает
    -------
    dict с ключами:
      • 'qty'            — требуемое количество контрактов (+ long, − short);
      • 'pnl_upper'      — совокупный PnL при upper_price;
      • 'pnl_lower'      — совокупный PnL при lower_price;
      • 'upper_share'    — доля pnl_upper в суммарном PnL;
      • 'lower_share'    — доля pnl_lower в суммарном PnL;
      • 'feasible_min'   — минимальное Q для обоих PnL ≥ 0;
      • 'feasible_max'   — максимальное Q для обоих PnL ≥ 0;
      • 'clipped'        — True, если qty подрезан до [feasible_min, feasible_max];
      • 'dominant_side'  — 'upper' или 'lower' (где должна быть доля share_target).
    """

    # --- Проверки входных параметров ----------------------------------------
    if not (lower_price < current_price < upper_price):
        raise ValueError("Должно выполняться: lower_price < current_price < upper_price.")
    if contract_multiplier <= 0:
        raise ValueError("contract_multiplier должен быть > 0.")
    if not (0 < share_target < 1):
        raise ValueError("share_target должен быть в диапазоне (0, 1).")
    if option_type.lower() not in {"put", "call"}:
        raise ValueError("option_type должен быть 'put' или 'call'.")
    if opt_abs_upper < 0 or opt_abs_lower < 0:
        raise ValueError("opt_abs_upper и opt_abs_lower должны быть неотрицательными.")

    option_type = option_type.lower()

    # --- Разницы цен --------------------------------------------------------
    U = upper_price - current_price  # движение вверх
    D = current_price - lower_price  # движение вниз

    # --- PnL опциона с нужным знаком и определение доминирующей стороны --------
    if option_type == "put":
        # put теряет наверху, зарабатывает внизу; фьючерс нужен в лонг, чтобы на верхней быть в плюсе
        O_u = -opt_abs_upper  # убыток наверху
        O_d =  opt_abs_lower  # прибыль внизу
        dominant_side = "upper"
    else:  # call
        # call зарабатывает наверху, теряет внизу; шорт фьючерса приносит плюс внизу
        O_u =  opt_abs_upper  # прибыль наверху
        O_d = -opt_abs_lower  # убыток внизу
        dominant_side = "lower"

    # --- Целевые соотношения -------------------------------------------------
    share_dom = share_target
    share_other = 1.0 - share_target

    # Нужно выразить отношение P_u / P_d. Оно зависит от того, где должна быть доля share_target:
    if dominant_side == "upper":
        ratio = share_dom / share_other  # хотим P_u / P_d = ratio
    else:  # lower доминирующий: хотим P_d / P_u = share_dom / share_other => P_u / P_d = share_other / share_dom
        ratio = share_other / share_dom

    # --- Вычисление целевого Q по соотношению --------------------------------
    denom = contract_multiplier * (U + ratio * D)
    if denom == 0:
        raise ValueError("Нулевой знаменатель — проверьте параметры цен и доли.")

    # Формула: (Q*cm*U + O_u) / (-Q*cm*D + O_d) = ratio  => Q = (ratio * O_d - O_u) / (cm*(U + ratio*D))
    Q_target = (ratio * O_d - O_u) / denom

    # --- Диапазон, в котором оба PnL >= 0 ------------------------------------
    # P_u = Q*cm*U + O_u >= 0  => Q >= -O_u / (cm*U)
    # P_d = -Q*cm*D + O_d >= 0 => Q <= O_d / (cm*D)
    Q_min = -O_u / (contract_multiplier * U)
    Q_max =  O_d / (contract_multiplier * D)

    if Q_min >= Q_max:
        raise ValueError("Не существует Q, при котором оба PnL положительны (неконсистентные входные данные).")

    # --- Клиппинг ------------------------------------------------------------
    clipped = False
    Q = Q_target
    if Q < Q_min:
        Q = Q_min
        clipped = True
    elif Q > Q_max:
        Q = Q_max
        clipped = True

    # --- Итоговые PnL и доли -------------------------------------------------
    pnl_upper = Q * contract_multiplier * U + O_u
    pnl_lower = -Q * contract_multiplier * D + O_d
    total = pnl_upper + pnl_lower
    upper_share = pnl_upper / total if total != 0 else float("nan")
    lower_share = pnl_lower / total if total != 0 else float("nan")

    return {
        "qty": abs(Q),  # положительное => long, отрицательное => short
        "pnl_upper": pnl_upper,
        "pnl_lower": pnl_lower,
        "upper_share": upper_share,
        "lower_share": lower_share,
        "feasible_min": Q_min,
        "feasible_max": Q_max,
        "clipped": clipped,
        "dominant_side": dominant_side,
    }


def time_to_expiry(delivery_ms: str) -> float:
    """
    Возвращает количество часов до экспирации опциона,
    рассчитанное по полю deliveryTime (миллисекунды с эпохи).
    """
    try:
        ms = int(delivery_ms)
    except (TypeError, ValueError):
        raise ValueError(f"Неверное значение deliveryTime: {delivery_ms!r}")
    expiry_dt = datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
    now = datetime.now(timezone.utc)
    delta = expiry_dt - now
    return delta.total_seconds() / 3600.0

def calc_bid(bids: Dict[float, float], percent: float) -> float:
    """
    Возвращает значение ставки по словарю ориентиров, производя линейную
    интерполяцию (или, при необходимости, экстраполяцию).

    :param bids: словарь вида {доля: значение}, например {0.01: 2.5, 0.025: 3.7}
    :param percent: доля в интервале 0.005–0.05 (0.5 %–5 %)
    :return: интерполированное или экстраполированное значение
    :raises ValueError: если percent вне [0.005, 0.05]
    """
    if not 0.005 <= percent <= 0.05:
        raise ValueError("percent должен быть в диапазоне от 0.005 до 0.05")

    # Сортируем ключи один раз; это работает для произвольного числа ориентиров.
    keys = sorted(bids)

    # Точное совпадение – вернём сразу.
    if percent in bids:
        return bids[percent]

    # Находим позицию, куда percent встанет в отсортированном массиве ключей.
    idx = bisect.bisect_left(keys, percent)

    # Определяем две соседние точки для интерполяции / экстраполяции.
    if idx == 0:                 # левее минимума – экстраполяция по первым двум точкам
        k0, k1 = keys[0], keys[1]
    elif idx == len(keys):       # правее максимума – экстраполяция по двум последним
        k0, k1 = keys[-2], keys[-1]
    else:                        # обычный случай – интерполяция между соседями
        k0, k1 = keys[idx - 1], keys[idx]

    v0, v1 = bids[k0], bids[k1]

    # Линейная интерполяция/экстраполяция.
    return v0 + (percent - k0) / (k1 - k0) * (v1 - v0)

def index(ask: float, strike: float, hours_to_exp: float, last_px: float, opt_type: str = 'put'):
    perc = (last_px-strike)/last_px if opt_type == 'put' else (strike-last_px)/last_px
    return ((hours_to_exp/perc)/(ask/last_px))*(1/last_px)




def get_next_friday_day(days: int = 4) -> Tuple[str, int]:
    """
    Возвращает строку вида '15JUL' — день и трёхбуквенную аббревиатуру месяца
    (на английском, заглавными), а также количество дней до этой даты.

    Параметр days определяет:
      • если days == 0: возвращает сегодняшний день;
      • если 1 <= days <= 3: возвращает дату через указанное количество дней (без учёта выходных);
      • если days > 3: рассчитывает ближайшую пятницу:
          – если до пятницы осталось меньше 4 дней — берётся пятница через неделю;
          – иначе — ближайшая пятница текущей недели.
    """
    today = datetime.now(timezone.utc).date()

    if days == 0:
        target = today
        delta = 0
    elif 1 <= days <= 3:
        target = today + timedelta(days=days)
        delta = days
    else:
        weekday = today.weekday()  # понедельник=0, пятница=4
        days_until_friday = (4 - weekday) % 7
        if days_until_friday < 4:
            days_until_friday += 7
        target = today + timedelta(days=days_until_friday)
        delta = 7

    eng_months = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN",
                  "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]
    month_str = eng_months[target.month - 1]

    return f"{target.day}{month_str}", delta


def filter_otm_options(
    options: List[Dict[str, str]],
    expiry_token: str,
    option_flag: str,
    count: int
) -> List[Dict[str, str]]:
    """
    Фильтрует список опционов по следующим критериям:
      1) истекают в дату, содержащую строку expiry_token (например, '29AUG');
      2) соответствуют типу 'C' (Call) или 'P' (Put);
      3) являются OTM (out‑of‑the‑money) по текущей цене базового актива;
      4) возвращает не более count штук, отсортированных по близости страйка к текущей цене:
         — для Call: от самых низких страйков выше цены;
         — для Put: от самых высоких страйков ниже цены.

    :param options: список словарей-описаний опционов
    :param expiry_token: фрагмент, определяющий дату экспирации (например, '29AUG')
    :param option_flag: 'C' для Call или 'P' для Put
    :param count: максимальное число опционов в результате
    :return: список отфильтрованных OTM опционов, не более count штук
    :raises ValueError: при неверном option_flag или отрицательном count
    """
    flag = option_flag.upper()
    if flag not in ('C', 'P'):
        raise ValueError("option_flag должен быть 'C' (Call) или 'P' (Put)")
    if count <= 0:
        raise ValueError("count должен быть положительным целым числом")

    # 1. Фильтрация по дате экспирации
    by_expiry = [
        opt for opt in options
        if expiry_token in opt.get('displayName', '') or expiry_token in opt.get('symbol', '')
    ]

    # 2. Фильтрация по типу
    type_map = {'C': 'Call', 'P': 'Put'}
    by_type = [opt for opt in by_expiry if opt.get('optionsType') == type_map[flag]]
    if not by_type:
        return []

    # 3. Вычисляем текущую цену базового актива (берём из первого)
    try:
        underlying_price = float(by_type[0]['underlyingPrice'])

    except (KeyError, TypeError, ValueError):
        raise ValueError("Не удалось получить корректную цену базового актива из данных опционов")

    # 4. OTM фильтрация и сортировка
    if flag == 'C':
        # OTM Call: strike > price
        otm = [opt for opt in by_type if float(opt['strike']) > underlying_price]
        # от самых близких к цене (минимальный страйк выше цены)
        sorted_otm = sorted(otm, key=lambda o: float(o['strike']))
    else:
        # OTM Put: strike < price
        otm = [opt for opt in by_type if float(opt['strike']) < underlying_price]
        # от самых близких к цене (максимальный страйк ниже цены)
        sorted_otm = sorted(otm, key=lambda o: float(o['strike']), reverse=True)

    # 5. Возвращаем первые count результатов
    return sorted_otm[:count]

def calc_tp_sl_pct(
    current_price: float,
    take_profit: float,
    stop_loss: float
):
    """
    Считает, на сколько процентов вверх лежит уровень take_profit
    от текущей цены, и на сколько процентов вниз — уровень stop_loss.
    Результаты возвращаются в виде дробей от единицы (0.01 = 1%).

    :param current_price: текущая цена инструмента (> 0)
    :param take_profit: цена тейк‑профита
    :param stop_loss: цена стоп‑лосса
    :return: кортеж (tp_pct, sl_pct),
             где tp_pct = (take_profit – current_price) / current_price,
                   sl_pct = (current_price – stop_loss) / current_price
    :raises ValueError: если current_price ≤ 0
    """
    if current_price <= 0:
        raise ValueError("current_price должен быть положительным числом")

    tp_pct = (take_profit - current_price) / current_price
    sl_pct = (current_price - stop_loss) / current_price

    return tp_pct, sl_pct

def calculate_percent_difference(close, high_or_low):
    # Small epsilon to avoid division by zero
    epsilon = 1e-6

    # Ensure close is not zero by replacing it with epsilon if necessary
    safe_close = close if abs(close) > epsilon else epsilon

    percent_difference = (high_or_low - safe_close) / safe_close

    return percent_difference


from typing import List, Dict, Tuple, Any

def option_competition(
    puts: List[Dict[str, Any]],
    calls: List[Dict[str, Any]],
    *,
    distance_metric: str = "percentage",
    tie_tolerance: float = 1e-9
) -> Tuple[List[Dict[str, Any]], str]:
    """
    Compare two equally-sized lists of put and call options that are (roughly) at-the-money (ATM)
    and determine which side is “cheaper” after normalising for the distance of each strike
    from the current underlying price.

    Parameters
    ----------
    puts, calls : List[Dict]
        Lists of option dictionaries in the exact format returned by the exchange API.
        `puts` must contain only «Put» contracts, and `calls` only «Call» contracts.
    distance_metric : {'percentage', 'absolute'}, optional
        • 'percentage' – distance = |strike - underlying| / underlying (default).  
        • 'absolute'   – distance = |strike - underlying|.
        The ask price of each option is divided by its distance.  Comparing these
        “price-per-unit-distance” figures puts both legs on equal footing even when the two
        strikes are not the same number of points / percent away.
    tie_tolerance : float, optional
        Treat differences smaller than this as a draw.

    Returns
    -------
    Tuple[List[Dict], str]
        • The list (either ``puts`` or ``calls``) that wins the most pair-wise duels.
          If the contest is a draw the function returns an empty list ``[]``.  
        • A plain-text report that records each round, normalised prices, running score,
          and the overall result.

    Notes
    -----
    1.  The two input lists need not be the same length; the shorter length sets
        the number of rounds.
    2.  Normalisation formula
            normalised_ask = ask_price / distance
        so the lower value represents “cheaper price per unit distance”.
    3.  The function **does not** mutate its inputs.

    Examples
    --------
    >>> winner, report = option_competition(puts, calls)
    >>> print(report)
    """

    if not puts or not calls:
        raise ValueError("Both `puts` and `calls` must contain at least one element.")

    # ── Helper -----------------------------------------------------------------
    def _distance(strike: float, underlying: float) -> float:
        if distance_metric == "percentage":
            return abs(strike - underlying) / underlying
        elif distance_metric == "absolute":
            return abs(strike - underlying)
        else:
            raise ValueError("distance_metric must be either 'percentage' or 'absolute'.")

    # ── Normalise and sort each side -------------------------------------------
    underlying_price = float(puts[0]["underlyingPrice"])
    sorted_puts  = sorted(puts,  key=lambda o: _distance(float(o["strike"]), underlying_price))
    sorted_calls = sorted(calls, key=lambda o: _distance(float(o["strike"]), underlying_price))

    rounds = min(len(sorted_puts), len(sorted_calls))
    score  = {"Put": 0, "Call": 0, "Draw": 0}
    lines  = ["Round | Strike(P) | Strike(C) | Dist(P) | Dist(C) | Ask(P) | Ask(C) "
              "| Norm(P) | Norm(C) | Winner"]

    for i in range(rounds):
        p, c = sorted_puts[i], sorted_calls[i]
        p_strike, c_strike = float(p["strike"]), float(c["strike"])
        p_ask,    c_ask    = float(p["askPrice"]), float(c["askPrice"])

        d_p = _distance(p_strike, underlying_price)
        d_c = _distance(c_strike, underlying_price)

        norm_p = p_ask / d_p if d_p else float("inf")
        norm_c = c_ask / d_c if d_c else float("inf")

        if abs(norm_p - norm_c) <= tie_tolerance:
            winner = "Draw"
        elif norm_p < norm_c:
            winner = "Put"
        else:
            winner = "Call"

        score[winner] += 1
        lines.append(
            f"{i+1:^5} | {p_strike:^10.1f}| {c_strike:^10.1f}| "
            f"{d_p:^7.4f}| {d_c:^7.4f}| {p_ask:^7.2f}| {c_ask:^7.2f}| "
            f"{norm_p:^8.2f}| {norm_c:^8.2f}| {winner}"
        )

    # ── Decide overall victor ---------------------------------------------------
    if score["Put"] > score["Call"]:
        overall_winner_list = puts
        overall = f"PUTs win {score['Put']} : {score['Call']}."
    elif score["Call"] > score["Put"]:
        overall_winner_list = calls
        overall = f"CALLs win {score['Call']} : {score['Put']}."
    else:
        overall_winner_list = []
        overall = f"Draw {score['Put']} : {score['Call']}."

    lines.append("─" * 93)
    lines.append(overall)

    report = "\n".join(lines)
    return overall_winner_list, report


def filter_options_by_distance(
    options: List[Dict[str, Any]],
    max_distance_pct: float
) -> List[Dict[str, Any]]:
    """
    Возвращает список опционов из `options`, у которых относительное
    отклонение strike от underlyingPrice не превышает max_distance_pct.

    :param options: список словарей с данными по опционам;
                    обязателен ключ 'strike' и ключ 'underlyingPrice'.
    :param max_distance_pct: максимально допустимая относительная разница
                             между strike и underlyingPrice (например, 0.01 = 1%).
    :return: отфильтрованный список опционов.
    """
    filtered: List[Dict[str, Any]] = []

    for opt in options:
        try:
            underlying = float(opt['underlyingPrice'])
            strike = float(opt['strike'])
        except (KeyError, TypeError, ValueError):
            # если данные некорректны или отсутствуют — пропускаем
            continue

        # вычисляем относительную разницу
        distance_pct = abs(strike - underlying) / underlying

        # оставляем только те опционы, где diff <= max_distance_pct
        if distance_pct <= max_distance_pct:
            filtered.append(opt)

    return filtered

def option_ask_indicator(
    hours_to_exp: float,
    strike_price: float,
    current_price: float,
    ask_price: float,
    option_type: Literal["call", "put"],
    rel_atr: Optional[float] = None,
    *,
    epsilon: float = 1e-6,
) -> float:
    """
    Индикатор выгодности ask-цены для опционов с оставшимся сроком ≤ 24 ч.

    ПАРАМЕТРЫ
    ---------
    hours_to_exp : float
        Часы до экспирации (0 < … ≤ 24).
    strike_price : float
        Страйк опциона.
    current_price : float
        Рыночная цена базового актива.
    ask_price : float
        Ask-цена опциона.
    option_type : {"call", "put"}
        Тип опциона.
    rel_atr : float | None, optional
        Относительный ATR последних 24 часов (ATR / price).  
        • Если передан None или ≤ 0 — волатильностная нормировка отключается.  
        • Можно передать оценку имплайд-волы в процентах (например, IV ≈ σ₁d).
    epsilon : float, optional
        Защита от деления на ноль.

    ВОЗВРАЩАЕТ
    ----------
    float
        < 1  — премия статистически дёшева;  
        ≈ 1  — справедлива;  
        > 1  — дорога.
    """
    # ---- Валидация ----

    if current_price <= 0 or ask_price < 0:
        raise ValueError("current_price must be > 0 and ask_price ≥ 0.")
    opt = option_type.lower()
    if opt not in ("call", "put"):
        raise ValueError("option_type must be 'call' or 'put' (case-insensitive).")

    # ---- 1.   Внутренняя и внешняя стоимость ----
    intrinsic = max(
        current_price - strike_price, 0.0
    ) if opt == "call" else max(
        strike_price - current_price, 0.0
    )
    extrinsic = max(ask_price - intrinsic, 0.0)
    if extrinsic < epsilon:
        return 0.0  # почти нет внешней стоимости – «дёшево»

    # ---- 2.   Премия за час в относительных единицах ----
    prem_per_hour = extrinsic / current_price / hours_to_exp

    # ---- 3.   Нормировка на волатильность (опционально) ----
    if rel_atr and rel_atr > epsilon:
        vol_per_hour = rel_atr / 24.0           # σ̂ _hour
        R = prem_per_hour / vol_per_hour
    else:
        R = prem_per_hour / epsilon            # условно «без волы»

    # ---- 4.   Moneyness-фактор для OTM-части ----
    if opt == "call":
        dist_pct = max(strike_price - current_price, 0.0) / current_price
    else:
        dist_pct = max(current_price - strike_price, 0.0) / current_price

    if rel_atr and rel_atr > epsilon:
        expected_move_pct = rel_atr * math.sqrt(hours_to_exp / 24.0)
    else:                                       # если вола неизвестна
        expected_move_pct = epsilon             # ≈ без коррекции
    expected_move_pct = max(expected_move_pct, epsilon)

    F = 1.0 + dist_pct / expected_move_pct

    # ---- 5.   Финальный индикатор ----
    return R * F



def dict_to_pretty_string(data: dict, indent: int = 0) -> str:
    """
    Convert a nested dict into an indented, human-readable string.
    Floats are rounded to 2 decimal places.
    """
    lines = []
    prefix = "  " * indent
    for key, value in data.items():
        if isinstance(value, dict):
            lines.append(f"{prefix}{key}:")
            lines.append(dict_to_pretty_string(value, indent + 1))
        else:
            display_value = round(value, 4) if isinstance(value, float) else value
            lines.append(f"{prefix}{key}: {display_value}")
    return "\n".join(lines)


def iv_index(askPx, strike, curent_px, iv, hours_to_exp):
    i1 = iv/hours_to_exp # iv за 1 час >
    p1 = iv/(abs(curent_px-strike)/curent_px) #iv за dist >
    ask_rel = askPx/curent_px
    ia1 = iv/ask_rel #iv за askPx >
    return i1*p1*ia1*0.1

def qty_for_target_profit(current_price: float, take_profit: float, target_profit: float) -> float:
    """
    Возвращает абсолютное количество (Qty) базового актива для линейного фьючерса,
    чтобы при движении цены на take_profit (доля, например 0.01 = 1%)
    прибыль составила target_profit (в котируемой валюте, например USDT).

    Работает одинаково для лонга и шорта. Направление позиции определяется тем,
    где расположен ваш TP относительно текущей цены (выше — лонг, ниже — шорт),
    но функция возвращает только |Qty|.

    Параметры:
      current_price > 0
      take_profit   > 0  (доля, а не проценты)
      target_profit > 0

    Пример:
      current_price=10_000, take_profit=0.01, target_profit=250  -> Qty = 2.5
    """
    if current_price <= 0:
        raise ValueError("current_price должен быть > 0")
    if take_profit <= 0:
        raise ValueError("take_profit должен быть > 0 (например, 0.01 для 1%)")
    if target_profit <= 0:
        raise ValueError("target_profit должен быть > 0")

    delta_per_unit = current_price * take_profit      # |изменение цены| на 1 единицу базового
    qty = target_profit / delta_per_unit              # требуемое количество по модулю
    return float(qty)
