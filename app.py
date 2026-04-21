"""
JTL Comparator — Flask-приложение для сравнения результатов нагрузочного тестирования JMeter.
Запуск: python app.py
Открыть: http://localhost:5000
"""

import json
import os
import tempfile
import threading
from configparser import ConfigParser
from pathlib import Path

from flask import Flask, render_template, request, jsonify

from analyzers.jtl_analyzer import parse_jtl, compare
from analyzers.jtl_jobs import JobRegistry
from analyzers.jtl_worker import (
    run_comparison_job,
    _PROGRESS_FILE,
    _RESULT_FILE,
    _ERROR_FILE,
    _DONE_FILE,
)

app = Flask(__name__)

# 4 ГБ — поддержка больших JTL-файлов
app.config["MAX_CONTENT_LENGTH"] = 4 * 1024 * 1024 * 1024

_JOBS_ROOT = Path(tempfile.gettempdir()) / "jtl-comparator" / "jobs"
_registry = JobRegistry(_JOBS_ROOT, completed_ttl_seconds=15 * 60)
_registry_lock = threading.Lock()

# Живые воркер-треды: job_id -> (Thread, cancel_event)
_workers: dict[str, tuple[threading.Thread, threading.Event]] = {}
_workers_lock = threading.Lock()


def _collect_delta_rules(form) -> dict:
    keys = [
        "time_warning_pct", "time_critical_pct", "time_improved_pct",
        "rps_warning_drop_pct", "rps_critical_drop_pct", "rps_improved_gain_pct",
        "err_warning_increase_pct", "err_critical_increase_pct", "err_improved_decrease_pct",
    ]
    return {k: form.get(k) for k in keys}


def _sync_job_from_disk(job_id: str) -> dict | None:
    """Read worker-written files and update registry to reflect current state."""
    with _registry_lock:
        snapshot = _registry.get_job(job_id)
    if snapshot is None:
        return None

    if snapshot["status"] in ("completed", "failed", "cancelled"):
        return snapshot

    work_dir = Path(snapshot["work_dir"])
    done_file = work_dir / _DONE_FILE
    error_file = work_dir / _ERROR_FILE
    progress_file = work_dir / _PROGRESS_FILE

    if done_file.exists():
        result_path = work_dir / _RESULT_FILE
        with _registry_lock:
            _registry.update_job(job_id, status="completed", stage="completed",
                                 message="Готово", progress_pct=100,
                                 result_path=result_path)
        with _workers_lock:
            _workers.pop(job_id, None)
        with _registry_lock:
            return _registry.get_job(job_id)

    if error_file.exists():
        try:
            err_msg = json.loads(error_file.read_text(encoding="utf-8")).get("error", "")
        except Exception:
            err_msg = "Неизвестная ошибка"
        with _registry_lock:
            _registry.update_job(job_id, status="failed", stage="failed",
                                 message="Ошибка", error=err_msg)
        with _workers_lock:
            _workers.pop(job_id, None)
        with _registry_lock:
            return _registry.get_job(job_id)

    if progress_file.exists():
        try:
            prog = json.loads(progress_file.read_text(encoding="utf-8"))
            with _registry_lock:
                _registry.update_job(job_id,
                                     stage=prog.get("stage"),
                                     message=prog.get("message"),
                                     progress_pct=prog.get("progress_pct"))
        except Exception:
            pass

    # Check if thread finished unexpectedly (no done/error file)
    with _workers_lock:
        worker_entry = _workers.get(job_id)
    if worker_entry is not None:
        thread, _ = worker_entry
        if not thread.is_alive():
            with _registry_lock:
                snap = _registry.get_job(job_id)
            if snap and snap["status"] not in ("completed", "failed", "cancelled"):
                with _registry_lock:
                    _registry.update_job(job_id, status="failed", stage="failed",
                                         message="Поток завершился неожиданно",
                                         error="Worker thread exited without result")
            with _workers_lock:
                _workers.pop(job_id, None)

    with _registry_lock:
        return _registry.get_job(job_id)


@app.errorhandler(413)
def request_entity_too_large(e):
    limit_gb = app.config["MAX_CONTENT_LENGTH"] / (1024 ** 3)
    return jsonify({"error": f"Файл слишком большой. Максимальный размер: {limit_gb:.0f} ГБ."}), 413


@app.route("/")
def index():
    """Отдаёт главную страницу приложения."""
    return render_template("index.html")


@app.route("/compare/jobs", methods=["POST"])
def create_job():
    """POST /compare/jobs — создаёт асинхронный job сравнения двух JTL."""
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
    delta_rules = _collect_delta_rules(request.form)

    with _registry_lock:
        snapshot = _registry.create_job()
    job_id = snapshot["job_id"]
    work_dir = Path(snapshot["work_dir"])

    try:
        p1 = work_dir / ("run1" + (Path(file1.filename).suffix or ".jtl"))
        p2 = work_dir / ("run2" + (Path(file2.filename).suffix or ".jtl"))
        file1.save(str(p1))
        file2.save(str(p2))
    except Exception as exc:
        with _registry_lock:
            _registry.update_job(job_id, status="failed", stage="failed",
                                 message="Ошибка сохранения файлов", error=str(exc))
        return jsonify({"error": f"Не удалось сохранить файлы: {exc}"}), 500

    cancel_event = threading.Event()
    thread = threading.Thread(
        target=run_comparison_job,
        kwargs=dict(work_dir=work_dir, path1=str(p1), path2=str(p2),
                    name1=name1, name2=name2, jtl_mode=jtl_mode,
                    delta_rules=delta_rules, cancel_event=cancel_event),
        daemon=True,
    )
    thread.start()

    with _registry_lock:
        _registry.start_worker(job_id, stage="upload_saved",
                               message="Файлы загружены, запуск анализа…")
    with _workers_lock:
        _workers[job_id] = (thread, cancel_event)

    return jsonify({"job_id": job_id, "status": "queued"}), 202


@app.route("/compare/jobs/<job_id>", methods=["GET"])
def get_job(job_id: str):
    """GET /compare/jobs/<job_id> — статус и результат job."""
    snapshot = _sync_job_from_disk(job_id)
    if snapshot is None:
        return jsonify({"error": "Job не найден."}), 404

    response = {
        "job_id": snapshot["job_id"],
        "status": snapshot["status"],
        "stage": snapshot["stage"],
        "message": snapshot["message"],
        "progress_pct": snapshot["progress_pct"],
    }

    if snapshot["status"] == "completed" and snapshot.get("result_path"):
        try:
            result_path = Path(snapshot["result_path"])
            response["result"] = json.loads(result_path.read_text(encoding="utf-8"))
        except Exception as exc:
            return jsonify({"error": f"Не удалось прочитать результат: {exc}"}), 500

    if snapshot["status"] == "failed":
        response["error"] = snapshot.get("error", "Неизвестная ошибка")

    return jsonify(response)


@app.route("/compare/jobs/<job_id>", methods=["DELETE"])
def cancel_job(job_id: str):
    """DELETE /compare/jobs/<job_id> — отмена job."""
    with _registry_lock:
        snapshot = _registry.get_job(job_id)
    if snapshot is None:
        return jsonify({"error": "Job не найден."}), 404

    with _workers_lock:
        worker_entry = _workers.pop(job_id, None)
    if worker_entry is not None:
        _, cancel_event = worker_entry
        cancel_event.set()

    with _registry_lock:
        cancelled = _registry.cancel_job(job_id)
    return jsonify(cancelled), 202


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
    Partial TLS_CERT/TLS_KEY env config is treated as an error.
    If both are missing, return None and run over HTTP.
    """
    import ssl

    env_cert = (os.getenv("TLS_CERT") or "").strip()
    env_key = (os.getenv("TLS_KEY") or "").strip()
    env_ca = (os.getenv("TLS_CA") or "").strip()

    if bool(env_cert) ^ bool(env_key):
        raise RuntimeError(
            "Partial TLS env configuration: TLS_CERT and TLS_KEY must be set together."
        )

    explicit_tls_config = bool(os.getenv("TLS_CONFIG", "").strip())
    tls_cfg = _load_tls_config()
    cert = env_cert or tls_cfg.get("TLS_CERT")
    key = env_key or tls_cfg.get("TLS_KEY")
    ca = env_ca or tls_cfg.get("TLS_CA")

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
