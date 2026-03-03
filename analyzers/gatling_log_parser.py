"""
Парсер файлов simulation.log для Gatling 3.8.x — 3.9.x.

Формат файла (tab-separated):
  RUN    \t simClass \t simId \t startTs \t description \t version
  USER   \t scenario \t START|END \t timestamp [...]
  REQUEST\t groups   \t name    \t startTs  \t endTs   \t OK|KO \t message
  GROUP  \t hierarchy\t startTs \t endTs    \t cumulatedRT \t OK|KO
  SIMULATION_END \t ...

Логика выбора записей:
  - Если есть GROUP-записи → берём их (аналог Transaction Controller в JMeter).
  - Если GROUP нет → берём REQUEST-записи (все запросы как есть).
"""

from pathlib import Path

import pandas as pd


_SEPARATOR = "\t"
_STATUS_OK = "OK"


def parse_gatling_log(filepath: str | Path) -> pd.DataFrame:
    """
    Читает Gatling simulation.log и возвращает нормализованный DataFrame.

    Возвращает DataFrame с колонками:
      label (str), elapsed (float, мс), success (bool), timeStamp (float, мс с эпохи)

    :param filepath: путь к файлу simulation.log
    :return: нормализованный DataFrame
    :raises ValueError: если файл не является корректным simulation.log
    """
    path = Path(filepath)

    request_rows: list[dict] = []
    group_rows: list[dict] = []

    try:
        with path.open(encoding="utf-8", errors="replace") as fh:
            for line in fh:
                line = line.rstrip("\n\r")
                if not line:
                    continue
                parts = line.split(_SEPARATOR)
                record_type = parts[0]

                if record_type == "REQUEST":
                    row = _parse_request_line(parts)
                    if row is not None:
                        request_rows.append(row)

                elif record_type == "GROUP":
                    row = _parse_group_line(parts)
                    if row is not None:
                        group_rows.append(row)

    except OSError as exc:
        raise ValueError(f"Не удалось прочитать файл «{path.name}»: {exc}") from exc

    # Выбираем, что использовать
    if group_rows:
        raw_rows = group_rows
    elif request_rows:
        raw_rows = request_rows
    else:
        raise ValueError(
            f"Файл «{path.name}» не содержит записей REQUEST или GROUP. "
            "Убедитесь, что это корректный Gatling simulation.log."
        )

    df = pd.DataFrame(raw_rows)
    df["elapsed"] = pd.to_numeric(df["elapsed"], errors="coerce")
    df["timeStamp"] = pd.to_numeric(df["timeStamp"], errors="coerce")
    df = df.dropna(subset=["elapsed", "timeStamp"])

    if df.empty:
        raise ValueError(f"Файл «{path.name}» не содержит корректных записей о транзакциях.")

    return df


def _parse_request_line(parts: list[str]) -> dict | None:
    """
    Парсит строку REQUEST.
    Формат: REQUEST \t groups \t name \t startTs \t endTs \t OK|KO \t message
    Индексы:    [0]      [1]      [2]     [3]       [4]      [5]      [6]
    """
    if len(parts) < 6:
        return None
    try:
        start_ts = int(parts[3])
        end_ts = int(parts[4])
        status = parts[5].strip()
    except (ValueError, IndexError):
        return None

    return {
        "label":     parts[2].strip(),
        "elapsed":   float(end_ts - start_ts),
        "success":   status == _STATUS_OK,
        "timeStamp": float(start_ts),
    }


def _parse_group_line(parts: list[str]) -> dict | None:
    """
    Парсит строку GROUP.
    Формат: GROUP \t groupHierarchy \t startTs \t endTs \t cumulatedRT \t OK|KO
    Индексы:  [0]        [1]            [2]       [3]        [4]           [5]
    """
    if len(parts) < 6:
        return None
    try:
        start_ts = int(parts[2])
        end_ts = int(parts[3])
        status = parts[5].strip()
    except (ValueError, IndexError):
        return None

    return {
        "label":     parts[1].strip(),   # полный путь иерархии, напр. "Login,Step1"
        "elapsed":   float(end_ts - start_ts),
        "success":   status == _STATUS_OK,
        "timeStamp": float(start_ts),
    }
