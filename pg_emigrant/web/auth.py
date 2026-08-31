"""Session-cookie authentication for the pg_emigrant web GUI.

The GUI can drop replication slots, truncate target tables and read the whole
configuration, so "it only listens on localhost" stops being a boundary the
moment anyone binds it to a real interface or puts a tunnel in front of it.
This module adds a login gate in front of every page and API route.

Design notes
------------
* **Fail closed.**  ``auth.enabled: true`` without a credential is a startup
  error, not a warning — the alternative is a GUI that believes it is protected
  and is not.  Conversely, leaving auth off is allowed (it is the pre-existing
  behaviour) but says so on every start.

* **Sessions, not HTTP Basic.**  Basic auth cannot log out, offers nowhere to
  show *who* is logged in, and makes the browser's own credential cache the
  session store.  A signed session cookie costs one template and gives a real
  logout.

* **SameSite=Lax** on the session cookie is what keeps a cookie-authenticated
  ``POST /api/action`` from being triggerable by another site: a cross-site
  form post carries no cookie, so it arrives unauthenticated.  This is why
  there is no separate CSRF token.

* **The throttle is per client address**, in memory.  It is not a defence
  against a distributed attacker; it is there so that a single script cannot
  walk the password list of a GUI someone exposed by accident.

Password verification accepts either a Werkzeug hash (preferred) or a
plaintext password, and both paths take a constant-time comparison — an early
`return False` on a username mismatch would leak valid usernames through
response timing.
"""

from __future__ import annotations

import secrets
import threading
import time
from dataclasses import dataclass, field
from functools import wraps
from typing import Callable, Optional

from flask import (
    Flask,
    current_app,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    url_for,
)
from werkzeug.security import check_password_hash, generate_password_hash

from pg_emigrant.config import WebAuthConfig
from pg_emigrant.utils import get_logger

log = get_logger(__name__)

#: Endpoints reachable without a session.  Everything else requires one.
_PUBLIC_ENDPOINTS = frozenset({"login", "static"})

#: Session key holding the authenticated username.
_SESSION_USER = "pg_emigrant_user"


class AuthConfigError(RuntimeError):
    """The authentication settings cannot be honoured — refuse to serve."""


def hash_password(password: str) -> str:
    """Return a Werkzeug password hash suitable for ``auth.password_hash``."""
    return generate_password_hash(password)


def verify_credentials(auth: WebAuthConfig, username: str, password: str) -> bool:
    """Check a submitted username/password against the configured credential.

    Both the username and the password are compared in constant time, and the
    password is always checked even when the username is already wrong, so the
    response time does not reveal which half failed.
    """
    user_ok = secrets.compare_digest(username or "", auth.username or "")

    if auth.password_hash:
        try:
            pass_ok = check_password_hash(auth.password_hash, password or "")
        except Exception as exc:
            # A malformed/unsupported hash must never authenticate anyone.
            log.error(
                "Could not verify the configured web.auth.password_hash (%s) — "
                "regenerate it with 'pg_emigrant hash-password'.", exc,
            )
            pass_ok = False
    else:
        pass_ok = secrets.compare_digest(password or "", auth.password or "")

    return user_ok and pass_ok


@dataclass
class _Throttle:
    """In-memory failed-login counter, keyed by client address.

    The GUI runs the Flask server with ``threaded=True``, so several login
    POSTs can land at once.  Incrementing the counter is a read-modify-write,
    which without the lock could lose a failure and hand an attacker extra
    attempts — the one direction a throttle must not be wrong in.
    """

    max_attempts: int
    lockout_seconds: int
    _failures: dict[str, tuple[int, float]] = field(default_factory=dict)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def locked_for(self, key: str) -> int:
        """Seconds remaining before *key* may try again (0 = not locked)."""
        if self.max_attempts <= 0:
            return 0
        with self._lock:
            entry = self._failures.get(key)
            if entry is None:
                return 0
            count, last = entry
            if count < self.max_attempts:
                return 0
            remaining = int(self.lockout_seconds - (time.monotonic() - last))
            if remaining <= 0:
                # Lockout expired — start the client over with a clean slate.
                self._failures.pop(key, None)
                return 0
            return remaining

    def record_failure(self, key: str) -> None:
        with self._lock:
            count, _ = self._failures.get(key, (0, 0.0))
            self._failures[key] = (count + 1, time.monotonic())

    def reset(self, key: str) -> None:
        with self._lock:
            self._failures.pop(key, None)


def _client_key() -> str:
    """Identify the client for throttling.

    ``remote_addr`` only — deliberately NOT X-Forwarded-For, which a client can
    set freely and would turn the throttle into a no-op (a different forged
    value per attempt).  Behind a reverse proxy every request therefore shares
    the proxy's address and the throttle becomes global rather than per-client;
    that is the safe direction to be wrong in.
    """
    return request.remote_addr or "unknown"


def init_auth(app: Flask, auth: Optional[WebAuthConfig]) -> bool:
    """Install the login gate on *app*.  Returns whether auth is active.

    Raises :class:`AuthConfigError` when the settings ask for authentication
    that cannot be provided, so the caller can refuse to start the server.
    """
    auth = auth or WebAuthConfig()

    if not auth.is_enabled:
        if auth.enabled is True:  # unreachable via is_enabled, kept explicit
            raise AuthConfigError("web.auth.enabled is true but is_enabled is false")
        app.config["EMIGRANT_AUTH"] = None
        log.warning(
            "Web GUI authentication is DISABLED — anyone who can reach this "
            "port can read the configuration and run destructive operations "
            "(teardown, bootstrap, drift fixes). Set web.auth.password_hash in "
            "the config file (generate one with 'pg_emigrant hash-password') "
            "to require a login."
        )
        return False

    if not auth.has_credential:
        raise AuthConfigError(
            "web.auth.enabled is true but neither web.auth.password_hash nor "
            "web.auth.password is set. Refusing to start a GUI that reports "
            "itself as protected while accepting any visitor. Generate a hash "
            "with 'pg_emigrant hash-password' and put it in password_hash."
        )
    if not auth.username:
        raise AuthConfigError("web.auth.username must not be empty when auth is enabled")

    # Catch an unusable hash NOW rather than at the first login attempt.  The
    # shipped config.yaml.example carries a "REPLACE_ME" placeholder, and
    # without this check the GUI starts happily and simply refuses every
    # correct password, with the real reason buried in the server log.
    if auth.password_hash and auth.password_hash.count("$") < 2:
        raise AuthConfigError(
            f"web.auth.password_hash does not look like a password hash "
            f"({auth.password_hash[:24]!r}...). Generate one with "
            f"'pg_emigrant hash-password' and paste the whole line — or use "
            f"web.auth.password if you really want a plaintext password."
        )

    if auth.password and not auth.password_hash:
        log.warning(
            "web.auth.password is a plaintext password. Prefer "
            "web.auth.password_hash — generate one with 'pg_emigrant hash-password'."
        )

    if auth.secret_key:
        app.secret_key = auth.secret_key
    else:
        # A per-process random key is secure; it just means sessions do not
        # survive a restart, and separate workers cannot share them.
        app.secret_key = secrets.token_urlsafe(48)
        log.info(
            "No web.auth.secret_key configured — generated a random one. "
            "Sessions will not survive a restart of the GUI; set secret_key to "
            "keep people logged in across restarts."
        )

    app.config.update(
        SESSION_COOKIE_HTTPONLY=True,
        # Lax is what stops another site from driving POST /api/action with the
        # visitor's cookie; see the module docstring.
        SESSION_COOKIE_SAMESITE="Lax",
        SESSION_COOKIE_SECURE=bool(auth.cookie_secure),
        PERMANENT_SESSION_LIFETIME=max(1, auth.session_timeout_minutes) * 60,
    )
    app.config["EMIGRANT_AUTH"] = auth
    app.config["EMIGRANT_AUTH_THROTTLE"] = _Throttle(
        max_attempts=auth.max_attempts, lockout_seconds=auth.lockout_seconds
    )

    _register_auth_routes(app)

    @app.before_request
    def _require_login():
        if current_app.config.get("EMIGRANT_AUTH") is None:
            return None
        if request.endpoint in _PUBLIC_ENDPOINTS:
            return None
        if session.get(_SESSION_USER):
            # Touch the session so the idle timeout slides forward.
            session.permanent = True
            session.modified = True
            return None
        if request.path.startswith("/api/"):
            # The dashboard polls these; a JSON 401 lets app.js send the user
            # to the login page instead of showing a wall of error toasts.
            return jsonify({"error": "Authentication required", "login_url": url_for("login")}), 401
        return redirect(url_for("login", next=request.full_path if request.query_string else request.path))

    @app.context_processor
    def _auth_context():
        return {
            "auth_enabled": current_app.config.get("EMIGRANT_AUTH") is not None,
            "current_user": session.get(_SESSION_USER),
        }

    log.info("Web GUI authentication is enabled for user %r", auth.username)
    return True


def _safe_next(target: Optional[str]) -> str:
    """Return a safe post-login redirect target.

    Only same-site, path-absolute targets are honoured; anything else (an
    absolute URL, a scheme-relative ``//evil.example``) falls back to the
    dashboard, so ``?next=`` cannot be used to bounce a freshly logged-in user
    off to another host.
    """
    if not target or not target.startswith("/") or target.startswith("//"):
        return url_for("dashboard")
    return target


def _register_auth_routes(app: Flask) -> None:
    @app.route("/login", methods=["GET", "POST"])
    def login():
        auth: WebAuthConfig = current_app.config["EMIGRANT_AUTH"]
        throttle: _Throttle = current_app.config["EMIGRANT_AUTH_THROTTLE"]
        next_url = request.args.get("next") or request.form.get("next")

        if session.get(_SESSION_USER):
            return redirect(_safe_next(next_url))

        if request.method == "POST":
            key = _client_key()
            locked = throttle.locked_for(key)
            if locked:
                log.warning("Rejected login from %s — locked out for %ds", key, locked)
                flash(
                    f"Too many failed attempts. Try again in {locked} second(s).",
                    "error",
                )
            else:
                username = request.form.get("username", "")
                password = request.form.get("password", "")
                if verify_credentials(auth, username, password):
                    throttle.reset(key)
                    session.clear()
                    session[_SESSION_USER] = auth.username
                    session.permanent = True
                    log.info("Web GUI login succeeded for %r from %s", auth.username, key)
                    return redirect(_safe_next(next_url))
                throttle.record_failure(key)
                log.warning("Web GUI login FAILED for %r from %s", username, key)
                flash("Invalid username or password.", "error")

        return render_template("login.html", next=next_url or ""), (
            200 if request.method == "GET" else 401
        )

    @app.route("/logout", methods=["POST", "GET"])
    def logout():
        session.clear()
        return redirect(url_for("login"))


def login_required(view: Callable) -> Callable:
    """Belt-and-braces decorator for routes registered outside the app factory.

    The global ``before_request`` hook already covers every endpoint; this
    exists so a route added later is still protected if it somehow bypasses it.
    """

    @wraps(view)
    def _wrapped(*args, **kwargs):
        if current_app.config.get("EMIGRANT_AUTH") is None:
            return view(*args, **kwargs)
        if not session.get(_SESSION_USER):
            if request.path.startswith("/api/"):
                return jsonify({"error": "Authentication required"}), 401
            return redirect(url_for("login", next=request.path))
        return view(*args, **kwargs)

    return _wrapped
