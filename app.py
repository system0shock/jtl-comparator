"""
JTL Comparator — Flask-приложение для сравнения результатов нагрузочного тестирования.
Поддерживает JMeter JTL, Gatling simulation.log и Gatling stats.js.
Запуск: python app.py
Открыть: http://localhost:5000
"""

import os
import tempfile
from pathlib import Path

from flask import Flask, render_template, request, jsonify

from analyzers.jtl_analyzer import parse_jtl, compare, compare_from_agg
from analyzers.gatling_log_parser import parse_gatling_log
from analyzers.gatling_json_parser import parse_gatling_json
from analyzers.file_detector import detect_file_type

app = Flask(__name__)

# Максимальный размер загружаемого файла — 200 МБ
app.config["MAX_CONTENT_LENGTH"] = 200 * 1024 * 1024


@app.route("/")
def index():
    """Отдаёт главную страницу приложения."""
    return render_template("index.html")


@app.route("/compare", methods=["POST"])
def compare_runs():
    """
    Принимает два файла нагрузочного тестирования и названия прогонов,
    возвращает JSON с результатами сравнения.

    Поддерживаемые форматы (определяются автоматически):
    - JMeter JTL (.jtl, .csv)
    - Gatling simulation.log (.log)
    - Gatling stats.js (.js) из HTML-отчёта

    Ожидаемые поля формы:
    - file1: первый файл
    - file2: второй файл
    - name1: название первого прогона (строка)
    - name2: название второго прогона (строка)
    """
    # Проверяем наличие файлов в запросе
    if "file1" not in request.files or "file2" not in request.files:
        return jsonify({"error": "Необходимо загрузить оба файла (Run 1 и Run 2)."}), 400

    file1 = request.files["file1"]
    file2 = request.files["file2"]

    if not file1.filename or not file2.filename:
        return jsonify({"error": "Файлы не выбраны."}), 400

    name1 = request.form.get("name1", "Run 1").strip() or "Run 1"
    name2 = request.form.get("name2", "Run 2").strip() or "Run 2"
    delta_rules = {
        "time_warning_pct": request.form.get("time_warning_pct"),
        "time_critical_pct": request.form.get("time_critical_pct"),
        "time_improved_pct": request.form.get("time_improved_pct"),
        "rps_warning_drop_pct": request.form.get("rps_warning_drop_pct"),
        "rps_critical_drop_pct": request.form.get("rps_critical_drop_pct"),
        "rps_improved_gain_pct": request.form.get("rps_improved_gain_pct"),
        "err_warning_increase_pct": request.form.get("err_warning_increase_pct"),
        "err_critical_increase_pct": request.form.get("err_critical_increase_pct"),
        "err_improved_decrease_pct": request.form.get("err_improved_decrease_pct"),
    }

    # Сохраняем загруженные файлы во временные файлы и анализируем
    tmp1 = tmp2 = None
    try:
        # Сохраняем файл 1
        suffix1 = Path(file1.filename).suffix or ".jtl"
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix1) as f1:
            file1.save(f1)
            tmp1 = f1.name

        # Сохраняем файл 2
        suffix2 = Path(file2.filename).suffix or ".jtl"
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix2) as f2:
            file2.save(f2)
            tmp2 = f2.name

        # Определяем тип файлов
        type1 = detect_file_type(tmp1)
        type2 = detect_file_type(tmp2)

        if type1 != type2:
            return jsonify({
                "error": (
                    f"Файлы разных форматов: Run 1 — {_type_label(type1)}, "
                    f"Run 2 — {_type_label(type2)}. "
                    "Оба файла должны быть одного формата."
                )
            }), 400

        # Парсим и сравниваем в зависимости от типа
        result = _run_comparison(tmp1, tmp2, type1, name1, name2, delta_rules)
        result["file_type"] = type1
        return jsonify(result)

    except ValueError as exc:
        # Ошибки формата файла — возвращаем понятное сообщение
        return jsonify({"error": str(exc)}), 422

    except Exception as exc:
        # Непредвиденные ошибки
        app.logger.exception("Ошибка при сравнении файлов")
        return jsonify({"error": f"Внутренняя ошибка сервера: {exc}"}), 500

    finally:
        # Удаляем временные файлы
        for tmp in (tmp1, tmp2):
            if tmp and os.path.exists(tmp):
                os.remove(tmp)


def _run_comparison(
    path1: str,
    path2: str,
    file_type: str,
    name1: str,
    name2: str,
    rules: dict,
) -> dict:
    """Парсит и сравнивает два файла в зависимости от их формата."""
    if file_type == "jtl":
        df1 = parse_jtl(path1)
        df2 = parse_jtl(path2)
        return compare(df1, df2, name1, name2, rules=rules)

    if file_type == "gatling_log":
        df1 = parse_gatling_log(path1)
        df2 = parse_gatling_log(path2)
        return compare(df1, df2, name1, name2, rules=rules)

    if file_type == "gatling_json":
        agg1 = parse_gatling_json(path1)
        agg2 = parse_gatling_json(path2)
        return compare_from_agg(agg1, agg2, name1, name2, rules=rules)

    raise ValueError(f"Неизвестный тип файла: {file_type}")


def _type_label(file_type: str) -> str:
    """Возвращает читаемое название типа файла."""
    return {
        "jtl": "JMeter JTL",
        "gatling_log": "Gatling simulation.log",
        "gatling_json": "Gatling stats.js",
    }.get(file_type, file_type)


if __name__ == "__main__":
    debug = os.getenv("FLASK_DEBUG", "0").strip().lower() in {"1", "true", "yes", "on"}
    host = os.getenv("HOST", "127.0.0.1")
    port = int(os.getenv("PORT", "5000"))
    app.run(debug=debug, host=host, port=port)
