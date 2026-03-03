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

# Лимиты строк по секциям промпта — адаптированы под контекстное окно каждой модели.
# Структура: {"critical": N, "warning": N, "improved": N}
_MODEL_SECTION_SIZES: dict[str, dict[str, int]] = {
    "GigaChat":       {"critical": 3, "warning": 3, "improved": 2},
    "GigaChat-Pro":   {"critical": 5, "warning": 5, "improved": 3},
    "GigaChat-Max":   {"critical": 8, "warning": 8, "improved": 5},
    "GigaChat-2":     {"critical": 8, "warning": 8, "improved": 5},
    "GigaChat-2-Pro": {"critical": 10, "warning": 10, "improved": 5},
    "GigaChat-2-Max": {"critical": 15, "warning": 15, "improved": 8},
}
_DEFAULT_SECTION_SIZES: dict[str, int] = {"critical": 3, "warning": 3, "improved": 2}

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


def build_prompt(data: dict, section_sizes: dict[str, int] | None = None) -> str:
    """
    Строит промпт для GigaChat на основе результатов /compare.

    Промпт структурирован по секциям severity (critical / warning / improved),
    содержит distribution-строку с полным распределением транзакций и секцию
    новых/выбывших транзакций. Это даёт LLM контекст масштаба независимо от
    общего числа транзакций.

    :param data:          словарь из compare() — поля name1, name2, summary, rows.
    :param section_sizes: лимиты строк по классам {"critical": N, "warning": N, "improved": N}.
                          По умолчанию используется _DEFAULT_SECTION_SIZES.
    :return:              строка промпта.
    """
    if section_sizes is None:
        section_sizes = _DEFAULT_SECTION_SIZES

    name1: str = data.get("name1", "Run 1")
    name2: str = data.get("name2", "Run 2")
    summary: dict | None = data.get("summary")
    rows: list[dict] = data.get("rows", [])

    def fmt(v: float | None, decimals: int = 1) -> str:
        return f"{v:.{decimals}f}" if v is not None else "—"

    # ── Сводная строка ─────────────────────────────────────────────────────────
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

    # ── Классификация строк по severity ────────────────────────────────────────
    both_rows   = [r for r in rows if r.get("avg_1") is not None and r.get("avg_2") is not None]
    only_r1_rows = [r for r in rows if r.get("avg_2") is None]
    only_r2_rows = [r for r in rows if r.get("avg_1") is None]

    by_class: dict[str, list[dict]] = {"critical": [], "warning": [], "neutral": [], "improved": []}
    for r in both_rows:
        by_class.setdefault(r.get("d_avg_class", "neutral"), []).append(r)
    # Внутри каждой группы — по убыванию |d_avg|
    for bucket in by_class.values():
        bucket.sort(key=lambda r: -(abs(r.get("d_avg") or 0)))

    n_critical = len(by_class["critical"])
    n_warning  = len(by_class["warning"])
    n_neutral  = len(by_class["neutral"])
    n_improved = len(by_class["improved"])
    n_only_r1  = len(only_r1_rows)
    n_only_r2  = len(only_r2_rows)
    total      = len(rows)

    distribution = (
        f"{n_critical} critical, {n_warning} warning, "
        f"{n_neutral} neutral, {n_improved} improved"
        + (f" | только в {name1}: {n_only_r1}" if n_only_r1 else "")
        + (f", только в {name2}: {n_only_r2}" if n_only_r2 else "")
    )

    # ── Построитель одной секции таблицы ───────────────────────────────────────
    def _section(title: str, bucket: list[dict], limit: int) -> str:
        if not bucket:
            return f"### {title}\n_Нет транзакций._\n"
        top = bucket[:limit]
        header = (
            f"| Транзакция | Avg {name1} | Avg {name2} | Δ Avg% | Δ p99% | Err% {name2} |\n"
            "|---|---|---|---|---|---|\n"
        )
        body = "\n".join(
            f"| {r['label']} | {fmt(r.get('avg_1'))} | {fmt(r.get('avg_2'))} "
            f"| {fmt(r.get('d_avg'))}% | {fmt(r.get('d_p99'))}% | {fmt(r.get('err_2'))}% |"
            for r in top
        )
        tail = f"\n_(показано {len(top)} из {len(bucket)})_" if len(bucket) > limit else ""
        return f"### {title}\n{header}{body}{tail}\n"

    critical_section = _section(
        f"Критические деградации ({n_critical})",
        by_class["critical"],
        section_sizes.get("critical", 3),
    )
    warning_section = _section(
        f"Предупреждения ({n_warning})",
        by_class["warning"],
        section_sizes.get("warning", 3),
    )
    improved_section = _section(
        f"Улучшения ({n_improved})",
        by_class["improved"],
        section_sizes.get("improved", 2),
    )

    # ── Новые / выбывшие транзакции ────────────────────────────────────────────
    drift_lines: list[str] = []
    if only_r1_rows:
        examples = ", ".join(r["label"] for r in only_r1_rows[:3])
        extra = f" и ещё {n_only_r1 - 3}" if n_only_r1 > 3 else ""
        drift_lines.append(f"- Выбыли из {name2} ({n_only_r1}): {examples}{extra}")
    if only_r2_rows:
        examples = ", ".join(r["label"] for r in only_r2_rows[:3])
        extra = f" и ещё {n_only_r2 - 3}" if n_only_r2 > 3 else ""
        drift_lines.append(f"- Новые в {name2} ({n_only_r2}): {examples}{extra}")
    drift_section = (
        "### Новые / выбывшие транзакции\n" + "\n".join(drift_lines) + "\n\n"
        if drift_lines else ""
    )

    prompt = f"""Ты эксперт по нагрузочному тестированию. Проанализируй результаты сравнения двух прогонов JMeter.

Прогон 1 (база): {name1}
Прогон 2 (новая версия): {name2}
Всего транзакций: {total} ({distribution})

## Сводные метрики
| Метрика | {name1} | {name2} | Δ% |
|---|---|---|---|
{summary_block}
## Детализация по категориям

{critical_section}
{warning_section}
{improved_section}
{drift_section}Дай анализ в 3–5 абзацах на русском языке:
1. Общая оценка — прогон 2 деградировал или улучшился относительно базы?
2. Критические проблемы (если есть): какие транзакции и насколько деградировали.
3. Что осталось стабильным или улучшилось.
4. Практические рекомендации — только конкретные выводы из данных таблицы, без общих советов.
Используй цифры из таблицы. Не давай рекомендаций, которые не следуют напрямую из приведённых данных."""

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

    section_sizes = _MODEL_SECTION_SIZES.get(model, _DEFAULT_SECTION_SIZES)
    prompt = build_prompt(data, section_sizes=section_sizes)

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
