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
        result = compare(df1, df2, name1, name2)
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


if __name__ == "__main__":
    app.run(debug=True, host="127.0.0.1", port=5000)
