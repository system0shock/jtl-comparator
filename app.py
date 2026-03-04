"""
JTL Comparator — Flask-приложение для сравнения результатов нагрузочного тестирования JMeter.
Запуск: python app.py
Открыть: http://localhost:5000
"""

import os
import tempfile
from configparser import ConfigParser
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
    jtl_mode = request.form.get("jtl_mode", "auto")
    if jtl_mode not in ("auto", "tc", "samplers"):
        jtl_mode = "auto"
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

        # Парсим оба файла с выбранным режимом фильтрации
        df1 = parse_jtl(tmp1, mode=jtl_mode)
        df2 = parse_jtl(tmp2, mode=jtl_mode)

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


def _build_ssl_context() -> "ssl.SSLContext | None":
    """
    Build SSL context from env and optional config file.
    TLS_CERT + TLS_KEY enable HTTPS.
    TLS_CA enables client certificate verification (mTLS).
    If TLS_CERT or TLS_KEY are missing, return None and run over HTTP.
    """
    import ssl

    explicit_tls_config = bool(os.getenv("TLS_CONFIG", "").strip())
    tls_cfg = _load_tls_config()
    cert = os.getenv("TLS_CERT") or tls_cfg.get("TLS_CERT")
    key = os.getenv("TLS_KEY") or tls_cfg.get("TLS_KEY")
    ca = os.getenv("TLS_CA") or tls_cfg.get("TLS_CA")

    if not cert or not key:
        if explicit_tls_config:
            raise RuntimeError(
                "TLS_CONFIG is set, but TLS_CERT/TLS_KEY are not resolved. "
                "Fix the config file or provide TLS_CERT and TLS_KEY."
            )
        return None

    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    ctx.load_cert_chain(certfile=cert, keyfile=key)

    if ca:
        ctx.verify_mode = ssl.CERT_REQUIRED
        ctx.load_verify_locations(cafile=ca)

    return ctx


def _load_tls_config() -> dict[str, str]:
    """
    Read TLS paths from INI config file.
    Default file: config/mtls.ini
    Section: [mtls]
    Keys: tls_cert, tls_key, tls_ca
    """
    cfg_env = os.getenv("TLS_CONFIG")
    cfg_path = (cfg_env if cfg_env is not None else "config/mtls.ini").strip()
    if not cfg_path:
        return {}
    explicit_tls_config = cfg_env is not None and bool(cfg_env.strip())

    cfg_file = Path(cfg_path)
    if not cfg_file.exists():
        if explicit_tls_config:
            raise RuntimeError(f"TLS_CONFIG file not found: {cfg_file}")
        return {}

    parser = ConfigParser()
    parser.read(cfg_file, encoding="utf-8")
    if not parser.has_section("mtls"):
        if explicit_tls_config:
            raise RuntimeError(f"TLS_CONFIG is invalid: missing [mtls] section in {cfg_file}")
        return {}

    mapping = {
        "TLS_CERT": "tls_cert",
        "TLS_KEY": "tls_key",
        "TLS_CA": "tls_ca",
    }
    loaded: dict[str, str] = {}
    for env_key, cfg_key in mapping.items():
        raw = parser.get("mtls", cfg_key, fallback="").strip()
        if not raw:
            continue
        value_path = Path(raw)
        if not value_path.is_absolute():
            value_path = (cfg_file.parent / value_path).resolve()
        loaded[env_key] = str(value_path)
    return loaded


if __name__ == "__main__":
    import ssl

    debug = os.getenv("FLASK_DEBUG", "0").strip().lower() in {"1", "true", "yes", "on"}
    host = os.getenv("HOST", "127.0.0.1")
    port = int(os.getenv("PORT", "5000"))
    ssl_ctx = _build_ssl_context()
    if ssl_ctx:
        if ssl_ctx.verify_mode == ssl.CERT_REQUIRED:
            print(f"mTLS enabled - https://{host}:{port}")
        else:
            print(f"HTTPS mode - https://{host}:{port}")
    else:
        print(f"HTTP mode - http://{host}:{port}")
    app.run(debug=debug, host=host, port=port, ssl_context=ssl_ctx)
