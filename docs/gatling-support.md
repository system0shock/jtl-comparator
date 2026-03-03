# Gatling Support — Feature Design

## Контекст

Добавлена поддержка файлов Gatling в JTL Comparator.
Сопоставима с существующей JMeter JTL функциональностью: те же метрики,
те же правила подсветки дельт, тот же интерфейс.

Совместимость: **Gatling 3.8.8 — 3.9.6** (Java DSL и Scala DSL).

---

## Поддерживаемые форматы Gatling

### 1. simulation.log

Основной лог-файл Gatling. Расположение:
```
results/<simulation-id>/simulation.log
```

Формат (tab-separated):
```
RUN    \t simClass \t simId \t startTs \t description \t version
USER   \t scenario \t START|END \t timestamp [...]
REQUEST\t groups   \t name    \t startTs  \t endTs   \t OK|KO \t message
GROUP  \t hierarchy\t startTs \t endTs    \t cumulatedRT \t OK|KO
SIMULATION_END \t ...
```

**Поля REQUEST:**
- `array[0]` = `REQUEST`
- `array[1]` = иерархия групп-родителей (comma-separated, может быть пустым)
- `array[2]` = имя запроса (→ `label`)
- `array[3]` = startTs (мс от эпохи) → `timeStamp`
- `array[4]` = endTs → `elapsed = endTs - startTs`
- `array[5]` = `OK` или `KO` → `success`

**Поля GROUP:**
- `array[0]` = `GROUP`
- `array[1]` = иерархия группы (→ `label`, напр. `"Login,Step1"`)
- `array[2]` = startTs → `timeStamp`
- `array[3]` = endTs → `elapsed = endTs - startTs`
- `array[4]` = cumulatedResponseTime (не используется)
- `array[5]` = `OK` или `KO` → `success`

**Логика выбора:** если есть GROUP-записи — использовать их (аналог JMeter Transaction Controller), иначе REQUEST.

### 2. stats.js (HTML-отчёт)

JavaScript-файл из HTML-отчёта Gatling. Расположение:
```
results/<simulation-id>/js/stats.js
```

Структура:
```javascript
var statsResults = {
  "type": "GROUP",
  "contents": {
    "requestName": {
      "type": "REQUEST",
      "name": "requestName",
      "stats": {
        "numberOfRequests": {"total": N, "ok": N, "ko": N},
        "meanResponseTime": {"total": N},
        "percentiles1":     {"total": N},  // p50
        "percentiles3":     {"total": N},  // p95
        "percentiles4":     {"total": N},  // p99
        "meanNumberOfRequestsPerSecond": {"total": N}
      }
    }
  }
}
```

**Ограничения stats.js:**
- Данные уже агрегированы — нельзя пересчитать перцентили
- `p90` недоступен (есть только p50, p75, p95, p99)
- Используется `compare_from_agg()` вместо `compare()` (минуя `aggregate()`)

---

## Архитектура

```
app.py
  ├── detect_file_type()  → 'jtl' | 'gatling_log' | 'gatling_json'
  ├── parse_jtl()              → raw DataFrame
  ├── parse_gatling_log()      → raw DataFrame  (→ compare())
  └── parse_gatling_json()     → agg DataFrame  (→ compare_from_agg())

jtl_analyzer.py
  ├── aggregate(df)            → agg DataFrame
  ├── compare_from_agg(agg1, agg2, ...)   ← ключевая функция с delta-логикой
  └── compare(df1, df2, ...)   → aggregate() × 2 → compare_from_agg()
```

### Нормализованный raw DataFrame (для simulation.log и JTL)
| Колонка | Тип | Описание |
|---|---|---|
| `label` | str | Имя транзакции |
| `elapsed` | float | Время ответа (мс) |
| `success` | bool | True = OK |
| `timeStamp` | float | Время начала (мс от эпохи) |

### Агрегированный DataFrame (для stats.js и вывода aggregate())
| Колонка | Тип | Описание |
|---|---|---|
| `label` | str | Имя транзакции |
| `samples` | int | Количество запросов |
| `avg` | float | Среднее время (мс) |
| `p50/p90/p95/p99` | float\|None | Перцентили |
| `min/max` | float | Минимум/максимум |
| `throughput` | float | RPS |
| `error_rate` | float | % ошибок |

---

## Автоопределение типа файла (file_detector.py)

1. Первая непустая строка начинается с `RUN\t`, `REQUEST\t`, `GROUP\t`, `USER\t` → `gatling_log`
2. Строка начинается с `var ` или `{` + контент содержит `numberOfRequests`/`percentiles1` → `gatling_json`
3. Первая строка — CSV-заголовок с `timeStamp`, `elapsed`, `label` → `jtl`
4. Иначе → `ValueError`

---

## Валидация на бэкенде

- Если `type1 != type2` → HTTP 400: _"Файлы разных форматов"_
- В JSON-ответ добавляется поле `file_type` (для badge в UI)

---

## UI изменения

- Поле загрузки принимает `.jtl,.csv,.log,.js`
- После сравнения — badge «🔍 Формат: Gatling simulation.log»
- Подзаголовок: «JMeter / Gatling»
- Подсказка при пустом состоянии обновлена
