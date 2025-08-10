from typing import List, Dict, Any
from datetime import datetime, timezone

def format_trades_report(rows: List[Dict[str, Any]]) -> str:
    """
    Компактный отчёт по списку сделок.

    Порядок полей:
      time_open (DD.MM HH:MM), stage, type_close, over_strike (%), profit, opt_profit, put_profit

    Правила:
      - В шапке выводятся названия колонок с эмодзи-индикаторами.
      - В строках сделок перед каждым значением показывается только соответствующий эмодзи.
      - Если 'put_profit' отсутствует, используется 'fut_profit' (если есть).
      - Числа форматируются со знаком и двумя знаками после запятой.
      - over_strike форматируется в процентах без лишних нулей.
      - 'UTC' из времени убран намеренно.
    """
    # Эмодзи-индикаторы и подписи шапки
    EMOJI = {
        "time": "🕒",
        "stage": "🧭",
        "close": "🔚",
        "over": "🎯",
        "pnl": "💵",
        "opt": "🪙",
        "fut": "📊",
    }
    HEADER = (
        f"{EMOJI['time']} Время | "
        f"{EMOJI['stage']} Стадия | "
        f"{EMOJI['close']} Закрытие | "
        f"{EMOJI['over']} OverStrike | "
        f"{EMOJI['opt']} Opt P&L | "
        f"{EMOJI['fut']} Fut P&L | "
        f"{EMOJI['pnl']} P&L"
    )

    def _parse_time_compact(s: Any) -> str:
        if not s:
            return "–"
        try:
            st = str(s).replace("T", " ")
            if st.endswith("Z"):
                st = st[:-1] + "+00:00"
            dt = datetime.fromisoformat(st)
            # Нормализуем к UTC (для единообразия), но метку "UTC" не показываем
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            dt = dt.astimezone(timezone.utc)
            return dt.strftime("%d.%m %H:%M")
        except Exception:
            return str(s)

    def _pct(v: Any) -> str:
        try:
            p = float(v) * 100.0
            if abs(p - round(p)) < 1e-9:
                return f"{int(round(p))}%"
            return f"{p:.1f}%"
        except Exception:
            return "–"

    def _num(v: Any) -> str:
        try:
            return f"{float(v):+,.2f}".replace(",", " ")
        except Exception:
            return "–"

    lines: List[str] = [HEADER]
    sum = 0
    for row in rows:
        time_s   = _parse_time_compact(row.get("time_open"))
        stage    = row.get("stage", "–")
        tclose   = row.get("type_close", "–")
        over     = _pct(row.get("over_strike"))
        profit   = _num(row.get("profit"))
        opt_pnl  = _num(row.get("opt_profit"))
        fut_raw  = row.get("fut_profit", row.get("fut_profit"))
        fut_pnl  = _num(fut_raw)
        sum+=float(profit)

        line = (
            f"{EMOJI['time']} {time_s} | "
            f"{EMOJI['stage']} {stage} | "
            f"{EMOJI['close']} {tclose} | "
            f"{EMOJI['over']} {over} | "
            f"{EMOJI['opt']} {opt_pnl} | "
            f"{EMOJI['fut']} {fut_pnl} | "
            f"{EMOJI['pnl']} {profit}"
        )
        lines.append(line)
    lines.append(f'\nSUM: {sum}')

    return "\n".join(lines)
