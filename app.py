"""
JTL Comparator — Flask-приложение для сравнения результатов нагрузочного тестирования JMeter.
Запуск: python app.py
Открыть: http://localhost:5000
"""

import os
import tempfile
from pathlib import Path

from flask import Flask, render_template, request, jsonify

from analyzers.jtl_analyzer import parse_jtl, compare
from analyzers.gigachat_client import generate_summary, _load_credentials, AVAILABLE_MODELS

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
    Принимает два JTL-файла и названия прогонов, возвращает JSON с результатами сравнения.

    Ожидаемые поля формы:
    - file1: первый .jtl файл
    - file2: второй .jtl файл
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

        # Парсим оба файла
        df1 = parse_jtl(tmp1)
        df2 = parse_jtl(tmp2)

        # Сравниваем и возвращаем результат
        result = compare(df1, df2, name1, name2, rules=delta_rules)
        return jsonify(result)

    except ValueError as exc:
        # Ошибки формата файла — возвращаем понятное сообщение
        return jsonify({"error": str(exc)}), 422

    except Exception as exc:
        # Непредвиденные ошибки
        app.logger.exception("Ошибка при сравнении JTL-файлов")
        return jsonify({"error": f"Внутренняя ошибка сервера: {exc}"}), 500

    finally:
        # Удаляем временные файлы
        for tmp in (tmp1, tmp2):
            if tmp and os.path.exists(tmp):
                os.remove(tmp)


@app.route("/summarize", methods=["POST"])
def summarize():
    """
    Принимает JSON-тело (результат /compare), возвращает AI-саммари от GigaChat.

    Ожидаемое тело запроса (application/json):
    {"name1": str, "name2": str, "rows": [...], "summary": {...}}
    """
    if not _load_credentials():
        return jsonify({
            "error": "GigaChat не настроен. Создайте файл gigachat.key в корне проекта."
        }), 503

    data = request.get_json(force=True, silent=True)
    if not data or "rows" not in data:
        return jsonify({"error": "Некорректный запрос: отсутствует поле rows."}), 400

    model = data.get("model", AVAILABLE_MODELS[0])

    try:
        text = generate_summary(data, model=model)
        return jsonify({"text": text})

    except RuntimeError as exc:
        return jsonify({"error": str(exc)}), 503

    except (ValueError, ConnectionError) as exc:
        return jsonify({"error": str(exc)}), 502

    except Exception as exc:
        app.logger.exception("Ошибка при генерации AI-саммари")
        return jsonify({"error": f"Внутренняя ошибка сервера: {exc}"}), 500


if __name__ == "__main__":
    debug = os.getenv("FLASK_DEBUG", "0").strip().lower() in {"1", "true", "yes", "on"}
    host = os.getenv("HOST", "127.0.0.1")
    port = int(os.getenv("PORT", "5000"))
    app.run(debug=debug, host=host, port=port)
