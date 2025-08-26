# small_tab_best.py
# -*- coding: utf-8 -*-
"""
Практичный стек для малых табличных данных (N≈100–1000) с временной структурой.

Новая стратегия:
- Без калибровки (сырые вероятности).
- Бинарная цель: profit>0 -> 1, profit<0 -> 0.
- Подбор порогов под прибыль на OOF:
  * thr_up — верхний порог для «1» (long), макс. триммированного среднего PnL.
  * thr_dn — нижний порог для «0» (short), макс. триммированного среднего PnL по (-profit).
  * Минимальное покрытие на каждое плечо.
- На holdout считаем PnL по long/short/total и метрики качества.

API:
-----
from small_tab_best import train_best_model, predict_proba, predict_label

bundle = train_best_model(records, test_size=0.2, n_splits=5, tx_cost=0.0)
p = predict_proba(bundle, records[0]["metrics"])               # сырая вероятность класса 1
y = predict_label(bundle, records[0]["metrics"], mode="pnl")   # 1 если p>=thr_up, иначе 0
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Sequence, Union

import numpy as np
import pandas as pd

from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, f1_score,
    roc_auc_score, confusion_matrix
)
from sklearn.inspection import permutation_importance

# Опционально CatBoost
_HAS_CATBOOST = False
try:
    from catboost import CatBoostClassifier
    _HAS_CATBOOST = True
except Exception:
    _HAS_CATBOOST = False


# ======================
# Служебные утилиты
# ======================

def _nanmean_cols(mat: np.ndarray) -> np.ndarray:
    with np.errstate(invalid="ignore"):
        counts = np.sum(~np.isnan(mat), axis=0)
        sums = np.nansum(mat, axis=0)
        out = np.divide(sums, counts, where=counts > 0)
        out[counts == 0] = np.nan
    return out


# ==============
# Бленд (без калибровки)
# ==============

class BlendPipeline:
    """
    Усреднение вероятностей нескольких обученных пайплайнов (сырые proba).
    Совместим с API sklearn-классификатора.
    """
    def __init__(self, members: Sequence[Pipeline], weights: Optional[Sequence[float]] = None):
        self.members = list(members)
        if weights is None or len(weights) != len(self.members):
            self.weights = np.ones(len(self.members), dtype=float) / max(1, len(self.members))
        else:
            w = np.asarray(weights, dtype=float)
            self.weights = (w / (w.sum() if w.sum() > 0 else 1.0))
        self._estimator_type = "classifier"
        self.classes_ = np.array([0, 1], dtype=int)

    def fit(self, X, y=None, **kwargs):
        return self

    def predict_proba(self, X: Union[pd.DataFrame, np.ndarray]) -> np.ndarray:
        probs = [m.predict_proba(X)[:, 1] for m in self.members]   # список (n,)
        mat = np.vstack(probs)                                     # (m, n)
        p = (mat.T @ self.weights).ravel()
        p = np.clip(p, 1e-9, 1 - 1e-9)
        return np.vstack([1.0 - p, p]).T

    def predict(self, X):
        p = self.predict_proba(X)[:, 1]
        return (p >= 0.5).astype(int)

    def score(self, X, y):
        p = self.predict_proba(X)[:, 1]
        if len(np.unique(y)) < 2:
            return 0.5
        return roc_auc_score(y, p)

    def get_params(self, deep=True):
        return {"members": self.members, "weights": self.weights}

    def set_params(self, **params):
        for k, v in params.items():
            setattr(self, k, v)
        return self


# =========================
# Вспомогательные структуры
# =========================

@dataclass
class ModelCandidate:
    name: str
    pipeline: Pipeline
    priority: int      # CatBoost(0) < GBDT(1) < LogReg(2)


@dataclass
class TrainedCandidate:
    name: str
    pipeline: Pipeline
    oof_prob: np.ndarray
    oof_metrics: Dict[str, float]
    priority: int


@dataclass
class BestBundle:
    name: str                          # имя выбранной модели или "blend"
    pipeline: Union[Pipeline, BlendPipeline]   # БЕЗ калибровки
    feature_names: List[str]
    classes_: np.ndarray               # [0,1]
    threshold_f1: float                # порог под F1 (на сырых)
    threshold_pnl: float               # porog long-only (thr_up) для совместимости
    threshold_up: float                # новый верхний порог
    threshold_dn: Optional[float]      # новый нижний порог (может быть None)

    holdout_report: Dict[str, Any]
    holdout_confusion: np.ndarray
    perm_importance: pd.DataFrame
    pair_synergy: pd.DataFrame

    meta: Dict[str, Any]


# =======================
# Подготовка данных/сплит
# =======================

def _records_to_df(records: List[Dict[str, Any]], drop_profit_eq_zero: bool = True) -> Tuple[pd.DataFrame, pd.Series, pd.DataFrame]:
    if not isinstance(records, list) or not records:
        raise ValueError("records должен быть непустым списком.")

    X_rows, y_rows, meta_rows = [], [], []
    for r in records:
        m = r.get("metrics")
        if not isinstance(m, dict):
            raise ValueError("Каждая запись должна содержать dict 'metrics'.")
        X_rows.append(m)
        p = r.get("profit")
        if p is None:
            raise ValueError("Каждая запись должна содержать 'profit'.")
        if drop_profit_eq_zero and p == 0:
            y_rows.append(np.nan)
        else:
            y_rows.append(1 if p > 0 else 0)

        meta_rows.append({
            "open_time": r.get("open_time"),
            "close_time": r.get("close_time"),
            "profit": float(p),
            "type_of_signal": r.get("type_of_signal"),
            "type_of_close": r.get("type_of_close"),
        })

    X = pd.DataFrame(X_rows)
    for c in X.columns:
        X[c] = pd.to_numeric(X[c], errors="coerce")
    X = X.reindex(sorted(X.columns), axis=1)

    y = pd.Series(y_rows, name="target").astype("float")
    meta = pd.DataFrame(meta_rows)

    mask = ~y.isna()
    X, y, meta = X.loc[mask].reset_index(drop=True), y.loc[mask].astype(int).reset_index(drop=True), meta.loc[mask].reset_index(drop=True)

    nunq = X.nunique(dropna=True)
    const_cols = nunq[nunq <= 1].index.tolist()
    if const_cols:
        X = X.drop(columns=const_cols)

    if X.shape[1] == 0:
        raise ValueError("Нет информативных признаков после фильтрации.")

    if "open_time" in meta and meta["open_time"].notna().all():
        order = np.argsort(meta["open_time"].values)
        X = X.iloc[order].reset_index(drop=True)
        y = y.iloc[order].reset_index(drop=True)
        meta = meta.iloc[order].reset_index(drop=True)

    return X, y, meta


def _forward_splits(n: int, n_splits: int, min_train_frac: float = 0.4, embargo: int = 0) -> List[Tuple[np.ndarray, np.ndarray]]:
    n_splits = max(3, int(n_splits))
    fold_size = n // (n_splits + 1)
    fold_size = max(1, fold_size)
    min_train = max(int(n * min_train_frac), fold_size)

    splits = []
    start = min_train
    while start + fold_size <= n:
        tr_end = max(0, start - embargo)
        tr_idx = np.arange(0, tr_end, dtype=int)
        va_idx = np.arange(start, min(start + fold_size, n), dtype=int)
        if len(va_idx) > 0 and len(tr_idx) >= 10:
            splits.append((tr_idx, va_idx))
        start += fold_size

    if len(splits) < 3 and n > 30:
        k = 3 - len(splits)
        for i in range(k):
            cut = min(n - (k - i) * fold_size, n - fold_size)
            tr_end = max(0, cut - embargo)
            tr = np.arange(0, tr_end, dtype=int)
            va = np.arange(cut, min(cut + fold_size, n), dtype=int)
            if len(va) > 0 and len(tr) >= 10:
                splits.append((tr, va))
    return splits


def _class_weights(y: pd.Series) -> np.ndarray:
    vals, cnt = np.unique(y, return_counts=True)
    freq = {v: c for v, c in zip(vals, cnt)}
    total = len(y)
    w = {cls: total / (len(vals) * freq[cls]) for cls in vals}
    return np.array([w[int(t)] for t in y], dtype=float)


def _time_decay_weights(n: int, decay: float = 0.0) -> np.ndarray:
    if decay <= 0:
        return np.ones(n, dtype=float)
    idx = np.arange(n, dtype=float)
    w = np.exp(decay * (idx - (n - 1)))
    return w


# ==========================
# Кандидаты и их обучение OOF
# ==========================

def _make_candidates(random_state: int = 42) -> List[ModelCandidate]:
    candidates: List[ModelCandidate] = []

    if _HAS_CATBOOST:
        cb = CatBoostClassifier(
            depth=4,
            learning_rate=0.05,
            iterations=800,
            l2_leaf_reg=4.0,
            loss_function="Logloss",
            random_seed=random_state,
            verbose=False
        )
        candidates.append(ModelCandidate("catboost_d4_lr005_i800_l2_4", Pipeline([
            ("imputer", SimpleImputer(strategy="median")),
            ("clf", cb),
        ]), priority=0))

    gbdt = GradientBoostingClassifier(
        learning_rate=0.05,
        n_estimators=400,
        max_depth=3,
        subsample=0.85,
        random_state=random_state,
    )
    candidates.append(ModelCandidate("sk_gbdt_d3_lr005_n400_ss085", Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        ("clf", gbdt),
    ]), priority=1))

    lr_l1 = LogisticRegression(
        penalty="l1",
        C=0.5,
        solver="saga",
        max_iter=5000,
        random_state=random_state,
        n_jobs=None
    )
    candidates.append(ModelCandidate("logreg_l1_C05", Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler", StandardScaler(with_mean=True, with_std=True)),
        ("clf", lr_l1),
    ]), priority=2))

    return candidates


def _fit_oof(candidate: ModelCandidate, X: pd.DataFrame, y: pd.Series, splits, weights: np.ndarray) -> TrainedCandidate:
    n = len(y)
    oof_prob = np.full(n, np.nan, dtype=float)

    for _, (tr, va) in enumerate(splits, start=1):
        pipe = candidate.pipeline
        try:
            pipe.fit(X.iloc[tr], y.iloc[tr], clf__sample_weight=weights[tr])  # type: ignore
        except Exception:
            pipe.fit(X.iloc[tr], y.iloc[tr])
        prob = pipe.predict_proba(X.iloc[va])[:, 1]
        oof_prob[va] = prob

    mask = ~np.isnan(oof_prob)
    y_oof = y.values[mask]
    p_oof = oof_prob[mask]

    pred = (p_oof >= 0.5).astype(int)
    metrics = {
        "oof_acc": accuracy_score(y_oof, pred),
        "oof_precision": precision_score(y_oof, pred, zero_division=0),
        "oof_recall": recall_score(y_oof, pred, zero_division=0),
        "oof_f1": f1_score(y_oof, pred, zero_division=0),
        "oof_auc": roc_auc_score(y_oof, p_oof) if len(np.unique(y_oof)) > 1 else np.nan,
        "oof_n": int(mask.sum())
    }
    return TrainedCandidate(candidate.name, candidate.pipeline, oof_prob, metrics, candidate.priority)


# ==========================
# Пороги под F1 и под прибыль (long/short)
# ==========================

def _pick_threshold_f1(p: np.ndarray, y: np.ndarray) -> float:
    grid = np.linspace(0.05, 0.95, 19)
    best_t, best_f1 = 0.5, -1.0
    for t in grid:
        f1 = f1_score(y, (p >= t).astype(int), zero_division=0)
        if f1 > best_f1:
            best_f1, best_t = f1, float(t)
    return best_t


def _trimmed_mean(x: np.ndarray, trim: float) -> float:
    if len(x) == 0:
        return np.nan
    x = np.sort(np.asarray(x, float))
    k = int(np.floor(trim * len(x)))
    if 2 * k < len(x) and k > 0:
        x = x[k:-k]
    return float(np.mean(x)) if len(x) else np.nan


def _pick_thr_up_long(p: np.ndarray, profit: np.ndarray, *, tx_cost: float, min_coverage: float, trim: float, grid_points: int = 41) -> Tuple[float, Dict[str, float]]:
    n = len(p)
    qs = np.linspace(0.55, 0.95, grid_points)  # верхние пороги
    thr_grid = np.quantile(p, qs)
    best_thr, best_score = 0.5, -1e18
    best = {"n": 0, "coverage": 0.0, "pnl_trim": float("nan"), "pnl_mean": float("nan")}
    min_take = max(1, int(np.ceil(min_coverage * n)))

    for thr in thr_grid:
        take = (p >= thr)
        k = int(take.sum())
        if k < min_take:
            continue
        pnl = profit[take] - tx_cost
        score = _trimmed_mean(pnl, trim)
        if np.isfinite(score) and score > best_score:
            best_score = score
            best_thr = float(thr)
            best = {"n": k, "coverage": k / n, "pnl_trim": float(score), "pnl_mean": float(np.mean(pnl))}

    return best_thr, best


def _pick_thr_dn_short(p: np.ndarray, profit: np.ndarray, *, tx_cost: float, min_coverage: float, trim: float, grid_points: int = 41) -> Tuple[Optional[float], Dict[str, float]]:
    n = len(p)
    qs = np.linspace(0.45, 0.05, grid_points)  # нижние пороги
    thr_grid = np.quantile(p, qs)
    best_thr, best_score = None, -1e18
    best = {"n": 0, "coverage": 0.0, "pnl_trim": float("nan"), "pnl_mean": float("nan")}
    min_take = max(1, int(np.ceil(min_coverage * n)))

    for thr in thr_grid:
        take = (p <= thr)
        k = int(take.sum())
        if k < min_take:
            continue
        pnl = (-profit[take]) - tx_cost  # short: переворачиваем знак прибыли
        score = _trimmed_mean(pnl, trim)
        if np.isfinite(score) and score > best_score:
            best_score = score
            best_thr = float(thr)
            best = {"n": k, "coverage": k / n, "pnl_trim": float(score), "pnl_mean": float(np.mean(pnl))}

    return best_thr, best


# ==========================
# Пермутационные важности и пары
# ==========================

def _perm_importance(estimator, X_test: pd.DataFrame, y_test: pd.Series, random_state: int = 42) -> pd.DataFrame:
    r = permutation_importance(estimator, X_test, y_test, scoring="roc_auc", n_repeats=50, random_state=random_state)
    tbl = (pd.DataFrame({"feature": X_test.columns,
                         "perm_importance_mean": r.importances_mean,
                         "perm_importance_std": r.importances_std})
           .sort_values("perm_importance_mean", ascending=False)
           .reset_index(drop=True))
    return tbl


def _pair_synergy_topk(X: pd.DataFrame, y: pd.Series, topk: int = 30, random_state: int = 42) -> pd.DataFrame:
    from sklearn.tree import DecisionTreeClassifier

    var = X.var().sort_values(ascending=False)
    feats = list(var.index[:min(topk, X.shape[1])])

    rows = []
    n = len(y)
    cut = max(int(n * 0.8), n - 20)
    X_tr, X_te = X.iloc[:cut], X.iloc[cut:]
    y_tr, y_te = y.iloc[:cut], y.iloc[cut:]

    for i in range(len(feats)):
        for j in range(i + 1, len(feats)):
            f1, f2 = feats[i], feats[j]
            stump1 = DecisionTreeClassifier(max_depth=1, random_state=random_state).fit(X_tr[[f1]], y_tr)
            stump2 = DecisionTreeClassifier(max_depth=1, random_state=random_state).fit(X_tr[[f2]], y_tr)
            auc1 = roc_auc_score(y_te, stump1.predict_proba(X_te[[f1]])[:, 1]) if len(np.unique(y_te)) > 1 else np.nan
            auc2 = roc_auc_score(y_te, stump2.predict_proba(X_te[[f2]])[:, 1]) if len(np.unique(y_te)) > 1 else np.nan
            best_single = np.nanmax([auc1, auc2])

            dt2 = DecisionTreeClassifier(max_depth=2, random_state=random_state).fit(X_tr[[f1, f2]], y_tr)
            auc_pair = roc_auc_score(y_te, dt2.predict_proba(X_te[[f1, f2]])[:, 1]) if len(np.unique(y_te)) > 1 else np.nan

            rows.append({
                "feat_a": f1, "feat_b": f2,
                "auc_pair": float(auc_pair) if not np.isnan(auc_pair) else np.nan,
                "auc_best_single": float(best_single) if not np.isnan(best_single) else np.nan,
                "synergy": (float(auc_pair) - float(best_single)) if (not np.isnan(auc_pair) and not np.isnan(best_single)) else np.nan
            })
    df = pd.DataFrame(rows).dropna().sort_values(["synergy", "auc_pair"], ascending=[False, False]).reset_index(drop=True)
    return df.head(50)


# ======================
# Главная обучающая ф-ция
# ======================

def train_best_model(
    records: List[Dict[str, Any]],
    *,
    test_size: float = 0.2,
    n_splits: int = 5,
    tx_cost: float = 0.0,
    random_state: int = 42,
    embargo: int = 0,
    time_decay: float = 0.0,
    allow_blend: bool = True,
    min_auc_gain_to_switch: float = 0.01,
    min_coverage_long: float = 0.25,     # минимальное покрытие для long
    min_coverage_short: float = 0.25,    # минимальное покрытие для short
    trim: float = 0.10                    # триммирование для среднего PnL
) -> BestBundle:
    """
    Подготовка → OOF-сравнение кандидатов → выбор порогов (сырые proba) → holdout → интерпретация.
    """
    np.random.seed(random_state)

    # 1) Данные
    X, y, meta = _records_to_df(records, drop_profit_eq_zero=True)
    n = len(y)
    assert n >= 60, "Для устойчивой forward-валидации желательно >= 60 наблюдений."
    profit_all = meta["profit"].values.astype(float)

    # 2) Сплиты OOF (forward + embargo)
    splits = _forward_splits(n, n_splits=n_splits, min_train_frac=0.4, embargo=embargo)
    if len(splits) < 3:
        raise RuntimeError("Недостаточно данных для forward-сплитов. Увеличьте выборку или уменьшите n_splits.")

    weights_classes = _class_weights(y)

    # 3) Кандидаты
    candidates = _make_candidates(random_state=random_state)

    # 4) OOF-обучение и сравнение
    trained: List[TrainedCandidate] = []
    for c in candidates:
        tc = _fit_oof(c, X, y, splits, weights_classes)
        trained.append(tc)

    # 5) Выбор лучшего по OOF-AUC; при «ничьей» — приоритет CatBoost > GBDT > LogReg
    def _key(t: TrainedCandidate):
        return (np.nan_to_num(t.oof_metrics.get("oof_auc", np.nan), nan=-1.0), -t.priority)
    trained_sorted = sorted(trained, key=_key, reverse=True)
    best_single = trained_sorted[0]

    # 6) Бленд по желанию (сырые вероятности)
    use_blend = False
    blend_member_names: List[str] = []
    oof_blend = None
    if allow_blend and len(trained_sorted) >= 2:
        mat = np.vstack([t.oof_prob for t in trained_sorted])   # (m, n)
        oof_blend = _nanmean_cols(mat)                          # (n,)
        mask = ~np.isnan(oof_blend)
        if mask.sum() > 0 and len(np.unique(y.values[mask])) > 1:
            auc_blend = roc_auc_score(y.values[mask], oof_blend[mask])
            if np.isfinite(auc_blend) and auc_blend >= best_single.oof_metrics["oof_auc"] + min_auc_gain_to_switch:
                use_blend = True
                blend_member_names = [t.name for t in trained_sorted]

    # 7) Пороги на OOF (сырые proba)
    if use_blend and oof_blend is not None:
        p_oof_raw = oof_blend
    else:
        p_oof_raw = best_single.oof_prob
    mask_oof = ~np.isnan(p_oof_raw)
    p_oof = p_oof_raw[mask_oof]
    y_oof = y.values[mask_oof]
    profit_oof = profit_all[mask_oof]

    thr_f1 = _pick_threshold_f1(p_oof, y_oof)

    thr_up, up_stats = _pick_thr_up_long(p_oof, profit_oof,
                                         tx_cost=tx_cost,
                                         min_coverage=min_coverage_long,
                                         trim=trim)
    thr_dn, dn_stats = _pick_thr_dn_short(p_oof, profit_oof,
                                          tx_cost=tx_cost,
                                          min_coverage=min_coverage_short,
                                          trim=trim)

    # 8) Holdout-тест (последние test_size по времени)
    cut = int(n * (1.0 - max(0.1, min(test_size, 0.49))))
    X_tr, X_te = X.iloc[:cut], X.iloc[cut:]
    y_tr, y_te = y.iloc[:cut], y.iloc[cut:]
    w_tr = _class_weights(y_tr) * _time_decay_weights(len(y_tr), time_decay)
    profit_te = profit_all[cut:]

    # финальная модель
    if use_blend and oof_blend is not None:
        members: List[Pipeline] = []
        for t in trained_sorted:
            pipe = t.pipeline
            try:
                pipe.fit(X_tr, y_tr, clf__sample_weight=w_tr)  # type: ignore
            except Exception:
                pipe.fit(X_tr, y_tr)
            members.append(pipe)
        final_estimator = BlendPipeline(members)
        name_used = f"blend_mean({', '.join(blend_member_names)})"
    else:
        pipe = best_single.pipeline
        try:
            pipe.fit(X_tr, y_tr, clf__sample_weight=w_tr)  # type: ignore
        except Exception:
            pipe.fit(X_tr, y_tr)
        final_estimator = pipe
        name_used = best_single.name

    # Предсказания на holdout
    prob_te = final_estimator.predict_proba(X_te)[:, 1]

    # Метрики классификации (для ориентира)
    if len(np.unique(y_te)) > 1:
        holdout_auc = float(roc_auc_score(y_te, prob_te))
    else:
        holdout_auc = np.nan

    pred_te_f1 = (prob_te >= thr_f1).astype(int)
    acc_f1 = accuracy_score(y_te, pred_te_f1)
    f1_f1 = f1_score(y_te, pred_te_f1, zero_division=0)
    prec_f1 = precision_score(y_te, pred_te_f1, zero_division=0)
    rec_f1 = recall_score(y_te, pred_te_f1, zero_division=0)

    # Оценка порогов на holdout (long/short/total)
    long_mask = (prob_te >= thr_up) if np.isfinite(thr_up) else np.zeros_like(prob_te, dtype=bool)
    short_mask = (prob_te <= thr_dn) if (thr_dn is not None) else np.zeros_like(prob_te, dtype=bool)

    def _pnl_stats(mask, pnl_raw, sign=+1.0):
        k = int(mask.sum())
        cov = k / max(1, len(mask))
        if k == 0:
            return {"n": 0, "coverage": 0.0, "pnl_trim": None, "pnl_mean": None}
        pnl = (sign * pnl_raw[mask]) - tx_cost
        return {
            "n": k,
            "coverage": cov,
            "pnl_trim": float(_trimmed_mean(pnl, trim)),
            "pnl_mean": float(np.mean(pnl))
        }

    long_stats_te = _pnl_stats(long_mask, profit_te, sign=+1.0)
    short_stats_te = _pnl_stats(short_mask, profit_te, sign=-1.0)

    # Суммарный PnL как средневзвешенный по числу сделок
    tot_n = long_stats_te["n"] + short_stats_te["n"]
    if tot_n > 0:
        pnl_total = (
            (long_stats_te["pnl_trim"] if long_stats_te["pnl_trim"] is not None else 0.0) * long_stats_te["n"] +
            (short_stats_te["pnl_trim"] if short_stats_te["pnl_trim"] is not None else 0.0) * short_stats_te["n"]
        ) / tot_n
    else:
        pnl_total = None

    # Для совместимости: confusion по long-only порогу (как раньше)
    pred_te_pnl = (prob_te >= thr_up).astype(int)
    cm = confusion_matrix(y_te, pred_te_pnl, labels=[0, 1])

    report = {
        "n_train": int(len(y_tr)),
        "n_test": int(len(y_te)),
        "model_chosen": name_used,

        "oof_auc_selected": float(roc_auc_score(y_oof, p_oof)) if len(np.unique(y_oof)) > 1 else np.nan,
        "holdout_auc": holdout_auc,

        # Классические метрики под F1 (для ориентира)
        "thr_f1": float(thr_f1),
        "test_acc_f1": float(acc_f1),
        "test_f1_f1": float(f1_f1),
        "test_precision_f1": float(prec_f1),
        "test_recall_f1": float(rec_f1),

        # Пороги новой стратегии (сырые proba)
        "thr_up": float(thr_up),
        "thr_dn": (float(thr_dn) if thr_dn is not None else None),
        # для обратной совместимости с внешним кодом:
        "thr_pnl": float(thr_up),

        # OOF-статистика по порогам (подбор)
        "oof_long": up_stats,
        "oof_short": dn_stats,

        # Holdout-статистика по порогам
        "holdout_long": long_stats_te,
        "holdout_short": short_stats_te,
        "holdout_total_pnl_trim": (float(pnl_total) if pnl_total is not None else None),
    }

    # 9) Пермутационная важность и пары
    perm = _perm_importance(final_estimator, X_te, y_te, random_state=random_state)
    synergy = _pair_synergy_topk(X, y, topk=min(30, X.shape[1]), random_state=random_state)

    bundle = BestBundle(
        name=name_used,
        pipeline=final_estimator,              # БЕЗ калибровки
        feature_names=list(X.columns),
        classes_=np.array([0, 1], dtype=int),

        threshold_f1=float(thr_f1),
        threshold_pnl=float(thr_up),          # для совместимости
        threshold_up=float(thr_up),
        threshold_dn=(float(thr_dn) if thr_dn is not None else None),

        holdout_report=report,
        holdout_confusion=cm,
        perm_importance=perm,
        pair_synergy=synergy,

        meta={
            "n_samples": int(len(y)),
            "n_features": int(X.shape[1]),
            "test_cut_index": int(cut),
            "forward_splits": int(len(splits)),
            "tx_cost": float(tx_cost),
            "catboost_used": bool(_HAS_CATBOOST),
            "embargo": int(embargo),
            "time_decay": float(time_decay),
            "allow_blend": bool(allow_blend),
            "min_auc_gain_to_switch": float(min_auc_gain_to_switch),
            "random_state": int(random_state),
            "min_coverage_long": float(min_coverage_long),
            "min_coverage_short": float(min_coverage_short),
            "trim": float(trim),
        }
    )
    return bundle


# =====================
# Инференс для продакшна
# =====================

def _row_from_metrics(feature_names: List[str], metrics: Dict[str, Any]) -> pd.DataFrame:
    row = {f: pd.to_numeric(metrics.get(f, np.nan), errors="coerce") for f in feature_names}
    return pd.DataFrame([row], columns=feature_names)

def predict_proba(bundle: BestBundle, metrics: Dict[str, Any]) -> float:
    """
    Возвращает СЫРУЮ вероятность класса 1 из финального оценщика (без калибровки).
    """
    X_row = _row_from_metrics(bundle.feature_names, metrics)
    return float(bundle.pipeline.predict_proba(X_row)[:, 1][0])

def predict_label(bundle: BestBundle, metrics: Dict[str, Any], mode: str = "pnl") -> int:
    """
    mode:
      - "pnl": 1 если p >= threshold_up, иначе 0 (long-only порог).
      - "f1" : 1 если p >= threshold_f1, иначе 0 (классический).
    """
    p = predict_proba(bundle, metrics)
    if mode == "f1":
        return int(p >= bundle.threshold_f1)
    else:
        return int(p >= bundle.threshold_up)



# ==================
# Пример запуска
# ==================

# if __name__ == "__main__":
#     bundle = train_best_model(records, test_size=0.25, n_splits=5, tx_cost=0.0,
#                               embargo=2, time_decay=0.02, allow_blend=True)
#     print(bundle.holdout_report)
#     print(bundle.perm_importance.head(10))
#     print(bundle.pair_synergy.head(10))
#     ex = records[0]["metrics"]
#     print("Proba:", predict_proba(bundle, ex))
#     print("Label (pnl):", predict_label(bundle, ex, mode="pnl"))
