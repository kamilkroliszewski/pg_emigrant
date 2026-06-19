"""Flask application factory and routes for the pg_emigrant web GUI.

The GUI is a thin presentation layer: pages and API endpoints delegate to the
helpers in :mod:`pg_emigrant.web.services`, which in turn call the same async
orchestration functions the CLI uses.  Long-running / mutating operations are
dispatched to a background :class:`~pg_emigrant.web.jobs.JobManager`.

Security: this server exposes the configuration and can trigger destructive
database operations.  It binds to ``127.0.0.1`` by default and ships without
authentication — do not expose it publicly without a reverse proxy + auth.
"""

from __future__ import annotations

from typing import Optional

from flask import (
    Flask,
    abort,
    jsonify,
    render_template,
    request,
)

from pg_emigrant.config import ReplicatorConfig, load_config
from pg_emigrant.utils import setup_logging
from pg_emigrant.web import services
from pg_emigrant.web.jobs import JobManager


def create_app(config_path: str = "config.yaml") -> Flask:
    """Build and configure the Flask application.

    The config is loaded once at startup.  If it cannot be loaded the app still
    starts (so the user gets a readable error in the browser) but mutating
    actions are refused.
    """
    setup_logging(False)  # ensure INFO-level logging so jobs can capture output

    app = Flask(__name__)
    app.config["EMIGRANT_CONFIG_PATH"] = config_path
    app.config["EMIGRANT_JOBS"] = JobManager()

    try:
        app.config["EMIGRANT_CFG"] = load_config(config_path)
        app.config["EMIGRANT_CFG_ERROR"] = None
    except Exception as exc:  # FileNotFoundError, ValidationError, yaml errors…
        app.config["EMIGRANT_CFG"] = None
        app.config["EMIGRANT_CFG_ERROR"] = str(exc)

    _register_routes(app)
    return app


def _cfg(app: Flask) -> Optional[ReplicatorConfig]:
    return app.config.get("EMIGRANT_CFG")


def _jobs(app: Flask) -> JobManager:
    return app.config["EMIGRANT_JOBS"]


def _require_cfg(app: Flask) -> ReplicatorConfig:
    cfg = _cfg(app)
    if cfg is None:
        abort(400, app.config.get("EMIGRANT_CFG_ERROR") or "Configuration not loaded")
    return cfg


def _register_routes(app: Flask) -> None:
    # ── Pages ─────────────────────────────────────────────────────────────────
    @app.route("/")
    def dashboard():
        cfg = _cfg(app)
        databases: list[str] = []
        discover_error: Optional[str] = None
        if cfg is not None:
            try:
                databases = services.list_databases(cfg)
            except Exception as exc:  # source server unreachable, bad creds, …
                discover_error = str(exc)
        return render_template(
            "dashboard.html",
            active="dashboard",
            databases=databases,
            discover_error=discover_error,
            cfg_error=app.config.get("EMIGRANT_CFG_ERROR"),
            config_path=app.config["EMIGRANT_CONFIG_PATH"],
        )

    @app.route("/database/<name>")
    def database_detail(name: str):
        return render_template(
            "database.html",
            active="dashboard",
            database=name,
            actions=services.ACTIONS,
            cfg_error=app.config.get("EMIGRANT_CFG_ERROR"),
        )

    @app.route("/config")
    def config_page():
        cfg = _cfg(app)
        config_data = services.masked_config(cfg) if cfg is not None else None
        return render_template(
            "config.html",
            active="config",
            config_data=config_data,
            config_path=app.config["EMIGRANT_CONFIG_PATH"],
            cfg_error=app.config.get("EMIGRANT_CFG_ERROR"),
        )

    @app.route("/jobs")
    def jobs_page():
        return render_template("jobs.html", active="jobs")

    # ── JSON API ──────────────────────────────────────────────────────────────
    @app.route("/api/databases")
    def api_databases():
        cfg = _require_cfg(app)
        return jsonify({"databases": services.list_databases(cfg)})

    @app.route("/api/status")
    def api_status_all():
        cfg = _require_cfg(app)
        sections_arg = request.args.get("sections")
        sections = sections_arg.split(",") if sections_arg else None
        return jsonify({"statuses": services.collect_all_status(cfg, sections)})

    @app.route("/api/status/<name>")
    def api_status(name: str):
        cfg = _require_cfg(app)
        sections_arg = request.args.get("sections")
        sections = sections_arg.split(",") if sections_arg else None
        return jsonify(services.collect_status(cfg, name, sections))

    @app.route("/api/detect-ddl/<name>")
    def api_detect_ddl(name: str):
        cfg = _require_cfg(app)
        return jsonify(services.drift_report(cfg, name))

    @app.route("/api/jobs")
    def api_jobs():
        return jsonify({"jobs": [j.to_dict() for j in _jobs(app).list()]})

    @app.route("/api/jobs/<job_id>")
    def api_job(job_id: str):
        job = _jobs(app).get(job_id)
        if job is None:
            abort(404, f"Job {job_id} not found")
        return jsonify(job.to_dict())

    @app.route("/api/action", methods=["POST"])
    def api_action():
        cfg = _require_cfg(app)
        payload = request.get_json(silent=True) or {}
        action = payload.get("action")
        database = payload.get("database") or None
        options = payload.get("options") or {}

        spec = services.ACTIONS.get(action)
        if spec is None:
            abort(400, f"Unknown action: {action!r}")

        try:
            coro_factory = services.build_action_coro(cfg, action, database, options)
        except ValueError as exc:
            abort(400, str(exc))

        name = f"{spec['label']}" + (f" — {database}" if database else "")
        job = _jobs(app).submit(name, coro_factory, database=database)
        return jsonify({"job_id": job.id, "status": job.status}), 202

    # ── Error handling: return JSON for API routes ────────────────────────────
    @app.errorhandler(400)
    @app.errorhandler(404)
    def _json_errors(err):
        if request.path.startswith("/api/"):
            return jsonify({"error": getattr(err, "description", str(err))}), err.code
        return err
