"""
Клиент GigaChat для генерации AI-саммари по результатам сравнения JTL-прогонов.

Поток работы:
1. Читает credentials из файла gigachat.key (фолбэк — env GIGACHAT_CREDENTIALS).
2. Получает Bearer-токен через OAuth (кешируется на время жизни токена).
3. Строит промпт из Summary + топ-N деградаций.
4. Отправляет запрос к GigaChat и возвращает текст ответа.
"""

import os
import time
import threading
import uuid
from pathlib import Path

import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning  # type: ignore[import-untyped]

# GigaChat использует российский CA, не входящий в стандартный certifi bundle.
# Для локального инструмента отключаем проверку сертификата.
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)  # type: ignore[attr-defined]

# ── Константы API ──────────────────────────────────────────────────────────────

_OAUTH_URL = "https://ngw.devices.sberbank.ru:9443/api/v2/oauth"
_API_URL   = "https://gigachat.devices.sberbank.ru/api/v1/chat/completions"
_MODEL     = "GigaChat"
_SCOPE     = "GIGACHAT_API_PERS"

# Доступные модели GigaChat (от лёгкой к тяжёлой)
AVAILABLE_MODELS: list[str] = [
    "GigaChat",
    "GigaChat-Pro",
    "GigaChat-Max",
    "GigaChat-2",
    "GigaChat-2-Pro",
    "GigaChat-2-Max",
]
_TIMEOUT   = 30  # секунды

# ── Файл с credentials ────────────────────────────────────────────────────────

# gigachat.key лежит в корне проекта (на уровень выше analyzers/)
_KEY_FILE = Path(__file__).parent.parent / "gigachat.key"


def _load_credentials() -> str | None:
    """
    Читает base64-credentials из gigachat.key.
    Если файл отсутствует, пробует переменную окружения GIGACHAT_CREDENTIALS.
    """
    if _KEY_FILE.exists():
        value = _KEY_FILE.read_text(encoding="utf-8").strip()
        if value:
            return value
    return os.getenv("GIGACHAT_CREDENTIALS")


# ── Кеш токена ────────────────────────────────────────────────────────────────

_token_cache: dict = {"token": None, "expires_at": 0.0}
_lock = threading.Lock()


def _get_access_token(credentials_b64: str) -> str:
    """
    Возвращает действующий Bearer-токен GigaChat.
    Обновляет токен за 60 секунд до истечения.
    """
    with _lock:
        if time.time() < _token_cache["expires_at"] - 60 and _token_cache["token"]:
            return _token_cache["token"]

        resp = requests.post(
            _OAUTH_URL,
            headers={
                "Authorization": f"Basic {credentials_b64}",
                "RqUID": str(uuid.uuid4()),
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
            },
            data=f"scope={_SCOPE}",
            timeout=_TIMEOUT,
            verify=False,
        )
        if resp.status_code != 200:
            raise ValueError(
                f"GigaChat OAuth вернул {resp.status_code}: {resp.text[:200]}"
            )

        payload = resp.json()
        _token_cache["token"] = payload["access_token"]
        # expires_at в миллисекундах от эпохи
        _token_cache["expires_at"] = payload.get("expires_at", 0) / 1000.0

        return _token_cache["token"]


# ── Построение промпта ────────────────────────────────────────────────────────

_CLASS_ORDER = {"critical": 0, "warning": 1, "neutral": 2, "improved": 3}


def build_prompt(data: dict, top_n: int = 5) -> str:
    """
    Строит промпт для GigaChat на основе результатов /compare.

    :param data:  словарь из compare() — поля name1, name2, summary, rows.
    :param top_n: максимальное количество транзакций в разделе деградаций.
    :return:      строка промпта.
    """
    name1: str = data.get("name1", "Run 1")
    name2: str = data.get("name2", "Run 2")
    summary: dict | None = data.get("summary")
    rows: list[dict] = data.get("rows", [])

    def fmt(v: float | None, decimals: int = 1) -> str:
        return f"{v:.{decimals}f}" if v is not None else "—"

    # ── Сводная строка ─────────────────────────────────────────────────────────
    summary_block = ""
    if summary:
        summary_block = (
            f"| Avg (мс)  | {fmt(summary.get('avg_1'))} | {fmt(summary.get('avg_2'))} | {fmt(summary.get('d_avg'))}% |\n"
            f"| p95 (мс)  | {fmt(summary.get('p95_1'))} | {fmt(summary.get('p95_2'))} | {fmt(summary.get('d_p95'))}% |\n"
            f"| p99 (мс)  | {fmt(summary.get('p99_1'))} | {fmt(summary.get('p99_2'))} | {fmt(summary.get('d_p99'))}% |\n"
            f"| RPS       | {fmt(summary.get('rps_1'))} | {fmt(summary.get('rps_2'))} | {fmt(summary.get('d_rps'))}% |\n"
            f"| Err%      | {fmt(summary.get('err_1'))} | {fmt(summary.get('err_2'))} | — |\n"
        )
    else:
        summary_block = "_Сводные данные недоступны._\n"

    # ── Топ деградаций ─────────────────────────────────────────────────────────
    # Берём строки с обоими прогонами, сортируем: critical → warning → neutral → improved,
    # внутри группы — по убыванию |d_avg|.
    both_rows = [r for r in rows if r.get("avg_1") is not None and r.get("avg_2") is not None]
    sorted_rows = sorted(
        both_rows,
        key=lambda r: (
            _CLASS_ORDER.get(r.get("d_avg_class", "neutral"), 2),
            -(abs(r["d_avg"]) if r.get("d_avg") is not None else 0),
        ),
    )
    top = sorted_rows[:top_n]

    if top:
        top_block = "\n".join(
            f"| {r['label']} | {fmt(r.get('avg_1'))} | {fmt(r.get('avg_2'))} "
            f"| {fmt(r.get('d_avg'))}% ({r.get('d_avg_class','—')}) "
            f"| {fmt(r.get('d_p99'))}% | {fmt(r.get('err_2'))}% |"
            for r in top
        )
        top_section = (
            "## Топ транзакций с деградацией\n"
            f"| Транзакция | Avg {name1} | Avg {name2} | Δ Avg% | Δ p99% | Err% {name2} |\n"
            "|---|---|---|---|---|---|\n"
            + top_block
        )
    else:
        top_section = "## Топ транзакций с деградацией\n_Нет данных для сравнения._"

    total = len(rows)
    only_r1 = sum(1 for r in rows if r.get("avg_2") is None)
    only_r2 = sum(1 for r in rows if r.get("avg_1") is None)

    prompt = f"""Ты эксперт по нагрузочному тестированию. Проанализируй результаты сравнения двух прогонов JMeter.

Прогон 1 (база): {name1}
Прогон 2 (новая версия): {name2}
Всего транзакций: {total} (только в Run1: {only_r1}, только в Run2: {only_r2})

## Сводные метрики
| Метрика | {name1} | {name2} | Δ% |
|---|---|---|---|
{summary_block}
{top_section}

Дай анализ в 3–5 абзацах на русском языке:
1. Общая оценка — прогон 2 деградировал или улучшился относительно базы?
2. Критические проблемы (если есть): какие транзакции и насколько деградировали.
3. Что осталось стабильным или улучшилось.
4. Практические рекомендации для инженера по нагрузочному тестированию.
Будь конкретным — используй цифры из таблицы."""

    return prompt


# ── Публичная функция ─────────────────────────────────────────────────────────


def generate_summary(data: dict, model: str = _MODEL) -> str:
    """
    Генерирует AI-саммари по результатам сравнения JTL-прогонов.

    :param data:  словарь из /compare (name1, name2, summary, rows, rules).
    :param model: название модели GigaChat (должна быть в AVAILABLE_MODELS).
    :return:      текст ответа GigaChat.
    :raises RuntimeError:    credentials не найдены или неизвестная модель.
    :raises ValueError:      ошибка API GigaChat (4xx).
    :raises ConnectionError: сетевая ошибка или таймаут.
    """
    if model not in AVAILABLE_MODELS:
        raise RuntimeError(
            f"Неизвестная модель: '{model}'. "
            f"Допустимые значения: {', '.join(AVAILABLE_MODELS)}."
        )
    credentials = _load_credentials()
    if not credentials:
        raise RuntimeError(
            "GigaChat credentials не найдены. "
            "Создайте файл gigachat.key в корне проекта."
        )

    try:
        token = _get_access_token(credentials)
    except requests.exceptions.RequestException as exc:
        raise ConnectionError(f"Не удалось получить токен GigaChat: {exc}") from exc

    prompt = build_prompt(data)

    try:
        resp = requests.post(
            _API_URL,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            json={
                "model": model,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.3,
            },
            timeout=_TIMEOUT,
            verify=False,
        )
    except requests.exceptions.Timeout as exc:
        raise ConnectionError("GigaChat не ответил в течение 30 секунд.") from exc
    except requests.exceptions.RequestException as exc:
        raise ConnectionError(f"Ошибка соединения с GigaChat: {exc}") from exc

    if resp.status_code != 200:
        raise ValueError(
            f"GigaChat API вернул {resp.status_code}: {resp.text[:300]}"
        )

    return resp.json()["choices"][0]["message"]["content"]
