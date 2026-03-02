"""Configuration: business types, OKVED mapping, color themes."""

from __future__ import annotations

from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Tuple

BASE_DIR = Path(__file__).resolve().parent.parent
TEMPLATES_DIR = BASE_DIR / "app" / "report" / "blocks"
REPORTS_DIR = BASE_DIR / "app" / "storage" / "reports"


class BusinessType(str, Enum):
    B2C_SERVICE = "B2C_SERVICE"       # Ресторан, отель, салон, клиника, фитнес
    B2C_PRODUCT = "B2C_PRODUCT"       # Ритейл, e-com, автодилер
    B2B_SERVICE = "B2B_SERVICE"       # SaaS, IT, консалтинг, юридика
    B2B_PRODUCT = "B2B_PRODUCT"       # Производство, машиностроение
    PLATFORM = "PLATFORM"             # Маркетплейс, агрегатор
    B2B_B2C_HYBRID = "B2B_B2C_HYBRID" # Косметика B2B+B2C, франшизы, D2C+опт


class ConfidenceLevel(str, Enum):
    FACT = "FACT"           # 🔒 Прямые данные из источника A/B
    CALC = "CALC"           # 🔢 Вычислено по формуле из фактов
    ESTIMATE = "ESTIMATE"   # ⚠ Вычислено с допущениями


class LifecycleStage(str, Enum):
    STARTUP = "startup"         # Убытки, рост >50%, фандрейзинг
    GROWTH = "growth"           # Рост >20%, CAPEX
    INVESTMENT = "investment"   # Стройка заводов, M&A, крупные CAPEX
    MATURE = "mature"           # Стабильная маржа, low CAPEX


# ОКВЭД → тип бизнеса
OKVED_MAP: dict[str, BusinessType] = {
    "55": BusinessType.B2C_SERVICE,
    "56": BusinessType.B2C_SERVICE,
    "86": BusinessType.B2C_SERVICE,
    "87": BusinessType.B2C_SERVICE,
    "88": BusinessType.B2C_SERVICE,
    "93": BusinessType.B2C_SERVICE,
    "96": BusinessType.B2C_SERVICE,
    "47": BusinessType.B2C_PRODUCT,
    "45.1": BusinessType.B2C_PRODUCT,
    "45.2": BusinessType.B2C_PRODUCT,
    "45.3": BusinessType.B2C_PRODUCT,
    "45.4": BusinessType.B2C_PRODUCT,
    "62": BusinessType.B2B_SERVICE,
    "63": BusinessType.B2B_SERVICE,
    "69": BusinessType.B2B_SERVICE,
    "70": BusinessType.B2B_SERVICE,
    "71": BusinessType.B2B_SERVICE,
    "72": BusinessType.B2B_SERVICE,
    "73": BusinessType.B2B_SERVICE,
    "74": BusinessType.B2B_SERVICE,
    "25": BusinessType.B2B_PRODUCT,
    "26": BusinessType.B2B_PRODUCT,
    "27": BusinessType.B2B_PRODUCT,
    "28": BusinessType.B2B_PRODUCT,
    "29": BusinessType.B2B_PRODUCT,
    "30": BusinessType.B2B_PRODUCT,
    "31": BusinessType.B2B_PRODUCT,
    "32": BusinessType.B2B_PRODUCT,
    "33": BusinessType.B2B_PRODUCT,
}


def detect_business_type(okved: str) -> BusinessType | None:
    """Determine business type from OKVED code (e.g. '62.01' → B2B_SERVICE)."""
    # Try exact match first, then 2-digit prefix
    if okved in OKVED_MAP:
        return OKVED_MAP[okved]
    prefix = okved.split(".")[0]
    if prefix in OKVED_MAP:
        return OKVED_MAP[prefix]
    # Try first 4 chars (e.g. '45.1')
    short = okved[:4].rstrip(".")
    if short in OKVED_MAP:
        return OKVED_MAP[short]
    return None


# Оси перцептуальной карты по типу
PERCEPTUAL_AXES: dict[BusinessType, tuple[str, str]] = {
    BusinessType.B2C_SERVICE: ("Цена", "Уникальность концепции"),
    BusinessType.B2C_PRODUCT: ("Цена", "Ширина ассортимента"),
    BusinessType.B2B_SERVICE: ("Цена", "Функциональность"),
    BusinessType.B2B_PRODUCT: ("Цена", "Кастомизация"),
    BusinessType.PLATFORM: ("Цена", "Качество"),
    BusinessType.B2B_B2C_HYBRID: ("Цена", "Качество"),
}


# KPI по типу бизнеса
KPI_BY_TYPE: dict[BusinessType, list[str]] = {
    BusinessType.B2C_SERVICE: [
        "RevPASH", "Средний чек", "Оборачиваемость столов/кресел",
        "Food cost / себестоимость", "LTV", "Возвращаемость",
        "ROE", "EBITDA margin", "Выручка/сотрудник", "Долговая нагрузка",
    ],
    BusinessType.B2C_PRODUCT: [
        "GMV", "Конверсия", "Средний чек", "LFL",
        "Товарооборот", "ROE", "EBITDA margin",
        "Выручка/сотрудник", "Долговая нагрузка",
    ],
    BusinessType.B2B_SERVICE: [
        "ARR", "MRR", "Churn", "CAC", "LTV", "NRR", "DAU/MAU",
        "ROE", "EBITDA margin", "Выручка/сотрудник", "Долговая нагрузка",
    ],
    BusinessType.B2B_PRODUCT: [
        "Маржинальность", "Загрузка мощностей", "OEE", "Цикл производства",
        "ROE", "EBITDA margin", "Выручка/сотрудник", "Долговая нагрузка",
    ],
    BusinessType.PLATFORM: [
        "GMV", "Take rate", "CAC", "LTV", "Churn",
        "DAU/MAU", "ROE", "EBITDA margin",
    ],
    BusinessType.B2B_B2C_HYBRID: [
        # B2C метрики
        "GMV (B2C)", "Средний чек (B2C)", "Конверсия (B2C)",
        # B2B метрики
        "Средний заказ (B2B)", "Активная база (B2B)", "LTV (B2B)",
        # Универсальные
        "ROE", "EBITDA margin", "Выручка/сотрудник", "Долговая нагрузка",
    ],
}


# Default color theme (gold dark)
THEME_DEFAULT = {
    "bg": "#0D0B0E",
    "bg2": "#151217",
    "bg3": "#1C1820",
    "card": "#1A1620",
    "card2": "#211D28",
    "border": "#2E2838",
    "border2": "#3D3548",
    "text": "#E8E4EC",
    "text2": "#A8A0B0",
    "text3": "#706880",
    "gold": "#C9A44C",
    "gold2": "#E8C46A",
    "red": "#D44040",
    "green": "#3DB86A",
    "blue": "#4A8FE0",
    "purple": "#9060D0",
    "orange": "#E08040",
    "teal": "#40B0A0",
}
