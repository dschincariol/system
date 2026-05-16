"""
FILE: http_transport.py

Low-level HTTP transport and response helpers.
"""

"""
HTTP transport layer only.

Does NOT contain business logic.
Delegates to injected:
- ROUTE_SPECS
- API_HANDLERS
- auth configuration
"""

import json
import hmac
import inspect
import ipaddress
import logging
import os
import re
import time
import traceback
from contextlib import nullcontext
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs, unquote

from engine.api.http_parsing import deny_if_shutdown
from engine.api.rate_limit import build_default_rate_limiter
from engine.runtime.failure_diagnostics import log_failure, normalize_root_cause_code
from engine.runtime.metrics import emit_counter, emit_timing

log = logging.getLogger(__name__)


class InsecureConfiguration(RuntimeError):
    """Raised when the HTTP API would expose unsafe production defaults."""


def _env_flag(name: str) -> str:
    return str(os.environ.get(name, "") or "").strip().lower()


if _env_flag("TS_ENV") == "production" and not str(
    os.environ.get("DASHBOARD_API_TOKEN", "") or ""
).strip():
    raise InsecureConfiguration(
        "DASHBOARD_API_TOKEN must be set when TS_ENV=production"
    )


_STATE_PAYLOAD_HINT_KEYS = frozenset(
    {
        "health",
        "ingestion",
        "services",
        "readiness",
        "timestamps",
        "critical_blockers",
        "root_cause_candidates",
        "system_stage",
        "prices",
        "events",
        "providers",
        "job_summary",
        "lifecycle",
        "alive",
        "db",
    }
)


def _coerce_http_status(*values, default=200):
    for value in values:
        if isinstance(value, bool):
            status = int(value)
        elif isinstance(value, int):
            status = int(value)
        elif isinstance(value, float):
            status = int(value)
        else:
            text = str(value or "").strip()
            signless = text[1:] if text[:1] in {"+", "-"} else text
            if not text or not signless.isdigit():
                continue
            status = int(text)
        if 100 <= status <= 599:
            return status
    if default is None:
        return None
    return int(default)


def _looks_like_state_payload(payload):
    if not isinstance(payload, dict):
        return False

    if any(key in payload for key in _STATE_PAYLOAD_HINT_KEYS):
        return True

    canonical_keys = (
        "status",
        "state",
        "mode",
        "execution_mode",
        "execution_allowed",
        "reasons",
    )
    return sum(1 for key in canonical_keys if key in payload) >= 3


def _map_error_to_status(error_code: str) -> int:
    code = str(error_code or "").strip().lower()
    if not code:
        return 500

    if "timeout" in code:
        return 504
    if "body_too_large" in code:
        return 413
    if code == "unauthorized" or code.endswith("_unauthorized"):
        return 401
    if code == "unauthorized_table" or code.startswith("unauthorized_table:"):
        return 400
    if "forbidden" in code:
        return 403
    if code == "unknown_endpoint" or "not_found" in code or "not_registered" in code:
        return 404
    if code.startswith("deprecated") or code.startswith("gone"):
        return 410
    if "rate_limit" in code or "cooldown" in code or "too_many_requests" in code:
        return 429
    if (
        code.startswith("missing_")
        or code.startswith("invalid_")
        or code.startswith("unsupported_")
        or code.startswith("bad_")
        or code.startswith("malformed_")
        or code.endswith("_required")
        or "validation" in code
        or "schema_invalid" in code
    ):
        return 400
    if "unavailable" in code or "unreachable" in code or "restart_blocked" in code:
        return 503
    return 500


def _request_storage_timeout_s(ctx=None) -> float:
    raw = None
    if isinstance(ctx, dict):
        raw = ctx.get("STORAGE_REQUEST_TIMEOUT_S")
    if raw is None:
        raw = os.environ.get("DASHBOARD_STORAGE_REQUEST_TIMEOUT_S") or os.environ.get("TS_API_STORAGE_TIMEOUT_S")
    try:
        return max(0.05, float(raw if raw is not None else 0.5))
    except Exception:
        return 0.5


def _storage_readiness_cache_s(ctx=None) -> float:
    raw = None
    if isinstance(ctx, dict):
        raw = ctx.get("STORAGE_READINESS_CACHE_S")
    if raw is None:
        raw = os.environ.get("DASHBOARD_STORAGE_READINESS_CACHE_S")
    try:
        return max(0.0, float(raw if raw is not None else 2.0))
    except Exception:
        return 2.0


def _storage_required_paths(ctx=None) -> frozenset[str]:
    raw = ()
    if isinstance(ctx, dict):
        raw = ctx.get("STORAGE_REQUIRED_PATHS") or ()
    if isinstance(raw, str):
        return frozenset(part.strip() for part in raw.split(",") if part.strip())
    try:
        return frozenset(str(part or "").strip() for part in raw if str(part or "").strip())
    except Exception:
        return frozenset()


def _storage_unavailable_response(*, endpoint: str, error: BaseException | None = None, readiness=None) -> dict:
    try:
        from engine.runtime.storage_pool import storage_unavailable_payload

        return storage_unavailable_payload(endpoint=endpoint, error=error, readiness=readiness)
    except Exception:
        detail = f"{type(error).__name__}: {error}" if error is not None else "runtime_storage_unavailable"
        return {
            "ok": False,
            "error": "storage_unavailable",
            "detail": detail,
            "endpoint": str(endpoint or ""),
            "meta": {"status": 503, "retryable": True, "ts_ms": int(time.time() * 1000)},
        }


def _is_storage_acquisition_exception(error: BaseException) -> bool:
    try:
        from engine.runtime.storage_pool import is_storage_acquisition_error

        return bool(is_storage_acquisition_error(error))
    except Exception:
        text = str(error or "").lower()
        return "couldn't get a connection" in text or "storagepooltimeout" in type(error).__name__.lower()


def _storage_known_unavailable() -> bool:
    try:
        from engine.runtime.storage_pool import storage_readiness_snapshot

        snapshot = storage_readiness_snapshot()
        return bool(snapshot.get("checked") and snapshot.get("ok") is False)
    except Exception:
        return False


def _derive_response_status(payload, default_status=200):
    default_status = _coerce_http_status(default_status, default=200)

    if not isinstance(payload, dict):
        return default_status

    meta = payload.get("meta")
    explicit_status = _coerce_http_status(
        (meta or {}).get("status") if isinstance(meta, dict) else None,
        (meta or {}).get("http_status") if isinstance(meta, dict) else None,
        payload.get("status_code"),
        payload.get("http_status"),
        default=None,
    )
    if explicit_status is not None:
        return explicit_status

    if payload.get("ok", True):
        return default_status

    if _looks_like_state_payload(payload):
        return default_status

    return _map_error_to_status(
        str(payload.get("error") or payload.get("root_cause_code") or "")
    )


class StreamingResponse:
    def __init__(self, *, status=200, headers=None, stream_fn=None):
        self.status = int(status)
        self.headers = dict(headers or {})
        self.stream_fn = stream_fn


_DESTRUCTIVE_ENDPOINT_PATHS = frozenset(
    {
        "/api/operator/emergency_stop",
        "/api/operator/restart_feeds",
        "/api/system/repair_schema",
    }
)


def _parse_trusted_proxy_networks(raw: str | None):
    networks = []
    for item in str(raw or "").split(","):
        text = item.strip()
        if not text:
            continue
        try:
            networks.append(ipaddress.ip_network(text, strict=False))
        except Exception:
            log.warning("Ignoring invalid TS_DASHBOARD_TRUSTED_PROXIES entry: %s", text)
    return tuple(networks)


def _ip_in_networks(ip_text: str, networks) -> bool:
    try:
        addr = ipaddress.ip_address(str(ip_text or "").strip())
    except Exception:
        return False
    return any(addr in network for network in networks)


def _is_loopback_ip(ip_text: str) -> bool:
    try:
        return bool(ipaddress.ip_address(str(ip_text or "").strip()).is_loopback)
    except Exception:
        return str(ip_text or "").strip() in ("127.0.0.1", "::1", "localhost")


def build_handler(ROUTE_SPECS, API_HANDLERS, dashboard_api_token, ctx=None, static_dir=None):
    """
    Builds and returns a configured HTTP request handler class.
    """

    _MAX_JSON_BODY_BYTES = int(
        os.environ.get("DASHBOARD_MAX_JSON_BODY_BYTES", "1048576")
    )

    dashboard_token = (dashboard_api_token or "").strip()
    trusted_proxy_networks = _parse_trusted_proxy_networks(
        os.environ.get("TS_DASHBOARD_TRUSTED_PROXIES", "")
    )
    injected_limiter = (ctx or {}).get("API_RATE_LIMITER") if isinstance(ctx, dict) else None
    rate_limiter = (
        injected_limiter
        if injected_limiter is not None and hasattr(injected_limiter, "check")
        else build_default_rate_limiter()
    )

    def _warn(scope: str, err: Exception, **extra):
        log_failure(
            log,
            event=f"http_transport_{scope}",
            code=normalize_root_cause_code(f"http_transport_{scope}"),
            message=str(err),
            error=err,
            level=logging.WARNING,
            component="engine.api.http_transport",
            extra=extra or None,
            include_health=False,
            persist=True,
        )

    # ------------------------------------------------------------
    # Normalize ROUTE_SPECS (supports dict and tuple formats)
    # ------------------------------------------------------------
    # Route normalization happens once at handler construction so request
    # dispatch stays cheap and business handlers receive a stable map.
    routes = {}
    template_routes = {}

    def _compile_route_template(path):
        names = []
        parts = []
        for token in re.split(r"(\{[^/{}]+\})", str(path or "")):
            if token.startswith("{") and token.endswith("}"):
                name = token[1:-1].strip()
                if not name:
                    parts.append(re.escape(token))
                    continue
                names.append(name)
                parts.append(f"(?P<{name}>[^/]+)")
            else:
                parts.append(re.escape(token))
        return re.compile("^" + "".join(parts) + "$"), tuple(names)

    for r in ROUTE_SPECS:

        if isinstance(r, dict):
            method = str(r.get("method", "")).upper()
            path = str(r.get("path", ""))
            handler = r.get("handler")
            if method and path and handler:
                if "{" in path and "}" in path:
                    regex, names = _compile_route_template(path)
                    template_routes.setdefault(method, []).append((path, regex, names, handler))
                else:
                    routes[(method, path)] = handler
            continue

        if isinstance(r, tuple) and len(r) >= 3:
            method = str(r[0]).upper()
            path = str(r[1])
            handler = r[2]
            if "{" in path and "}" in path:
                regex, names = _compile_route_template(path)
                template_routes.setdefault(method, []).append((path, regex, names, handler))
            else:
                routes[(method, path)] = handler
            continue

    _STATIC_DIR = os.path.abspath(static_dir or os.getcwd())

    def _call_handler(fn, *, method, parsed, body, handler_ctx):
        try:
            sig = inspect.signature(fn)
            params = [
                p
                for p in sig.parameters.values()
                if p.kind in (
                    inspect.Parameter.POSITIONAL_ONLY,
                    inspect.Parameter.POSITIONAL_OR_KEYWORD,
                )
            ]
            has_varargs = any(
                p.kind == inspect.Parameter.VAR_POSITIONAL
                for p in sig.parameters.values()
            )
        except Exception as e:
            _warn("handler_signature", e, fn=getattr(fn, "__name__", "unknown"))
            params = []
            has_varargs = True

        argc = len(params)

        if method == "GET":
            if has_varargs or argc >= 3:
                return fn(parsed, None, handler_ctx)
            if argc == 2:
                return fn(parsed, handler_ctx)
            if argc == 1:
                return fn(parsed)
            return fn()

        if has_varargs or argc >= 3:
            return fn(parsed, body, handler_ctx)
        if argc == 2:
            return fn(parsed, body)
        if argc == 1:
            return fn(parsed)
        return fn()

    # ------------------------------------------------------------
    # Handler Class
    # ------------------------------------------------------------
    class Handler(SimpleHTTPRequestHandler):

        ROUTES = routes
        TEMPLATE_ROUTES = template_routes

        def __init__(self, *args, **kwargs):
            self._ctx = ctx or {}
            self._response_status = None
            self._response_ok = None
            self._response_output_valid = None
            self._response_streaming = False
            self._request_body_valid = True
            try:
                super().__init__(*args, directory=_STATIC_DIR, **kwargs)
            except TypeError:
                super().__init__(*args, **kwargs)

            try:
                self.connection.settimeout(15.0)
            except Exception as e:
                _warn("socket_timeout_set", e)

        # --------------------------------------------------------
        # Helpers
        # --------------------------------------------------------

        def log_message(self, format, *args):
            # disable default noisy logging
            return

        def _normalize_ui_legacy_path(self):
            try:
                parsed = urlparse(self.path)
                path = parsed.path or "/"
                static_root_name = os.path.basename(os.path.normpath(_STATIC_DIR))
                dashboard_path = "/dashboard.html" if static_root_name == "ui" else "/ui/dashboard.html"
                terminal_path = "/terminal/terminal.html" if static_root_name == "ui" else "/ui/terminal/terminal.html"

                if path in ("/", "/dashboard", "/dashboard.html"):
                    self.path = dashboard_path
                    return

                if path in ("/ui", "/ui/"):
                    self.path = dashboard_path
                    return

                if path in ("/terminal", "/terminal.html"):
                    self.path = terminal_path
                    return

                # allow /ui/... when static root is project root
                if path.startswith("/ui/"):
                    candidate = os.path.join(_STATIC_DIR, path.lstrip("/"))

                    if os.path.exists(candidate):
                        return

                    # when static root == ui directory
                    if static_root_name == "ui":
                        trimmed = path[3:] or "/"
                        if parsed.query:
                            trimmed = f"{trimmed}?{parsed.query}"
                        self.path = trimmed
                        return
            except Exception as e:
                _warn("normalize_ui_legacy_path", e, path=getattr(self, "path", ""))

        def _read_json_body(self):

            try:
                n = int(self.headers.get("Content-Length") or "0")
            except Exception as e:
                _warn("body_content_length_parse", e)
                n = 0

            if n <= 0:
                return None

            if n > _MAX_JSON_BODY_BYTES:
                return {
                    "__body_error__": "body_too_large",
                    "__body_bytes__": n,
                }

            try:
                raw = self.rfile.read(n)
            except Exception as e:
                _warn("body_read", e)
                return {"__body_error__": "body_read_failed"}

            try:
                return json.loads(
                    raw.decode("utf-8", errors="strict") or "{}"
                )
            except Exception as e:
                _warn("body_json_decode", e)
                return {"__body_error__": "invalid_json"}

        def _record_response_observation(
            self,
            *,
            status,
            ok,
            output_valid,
            streaming=False,
        ):
            try:
                self._response_status = int(status)
            except Exception:
                self._response_status = None
            self._response_ok = None if ok is None else bool(ok)
            self._response_output_valid = bool(output_valid)
            self._response_streaming = bool(streaming)

        def respond_json(self, obj, status=200, headers=None):
            if isinstance(obj, dict):
                obj = dict(obj)
            elif obj is None:
                obj = {"ok": False, "error": "empty_response"}
            else:
                obj = {"ok": bool(int(status) < 400), "data": obj}

            status = _derive_response_status(obj, default_status=status)

            # Normalize envelope shape centrally so endpoint handlers can
            # focus on business payloads instead of response boilerplate.
            ok = obj.get("ok")
            if ok is None:
                obj["ok"] = bool(int(status) < 400)
            if obj.get("ok", True):
                obj.setdefault("error", None)
            else:
                obj["error"] = str(obj.get("error") or "request_failed")
            meta = obj.get("meta")
            if not isinstance(meta, dict):
                meta = {}
            meta.setdefault("status", int(status))
            obj["meta"] = meta

            try:
                data = json.dumps(
                    obj,
                    separators=(",", ":"),
                    sort_keys=True,
                    default=str,
                ).encode("utf-8")
            except Exception as e:
                _warn("response_json_encode", e, status=status)
                data = b'{"ok":false,"error":"json_encode_failed","meta":{"status":500}}'
                status = 500
                obj = {"ok": False, "error": "json_encode_failed", "meta": {"status": 500}}

            self._record_response_observation(
                status=status,
                ok=obj.get("ok") if isinstance(obj, dict) else None,
                output_valid=isinstance(obj, dict) and "meta" in obj and "ok" in obj,
            )

            try:
                req_origin = (self.headers.get("Origin") or "").strip()
            except Exception as e:
                _warn("origin_header_read", e)
                req_origin = ""

            allow_origin = "http://127.0.0.1:8000"
            if req_origin in ("http://127.0.0.1:8000", "http://localhost:8000"):
                allow_origin = req_origin

            try:
                self.send_response(int(status))
                self.send_header(
                    "Content-Type", "application/json; charset=utf-8"
                )
                self.send_header("Cache-Control", "no-store")
                self.send_header("Access-Control-Allow-Origin", allow_origin)
                self.send_header("Vary", "Origin")
                self.send_header(
                    "Access-Control-Allow-Headers",
                    "Content-Type, X-API-Token",
                )
                self.send_header(
                    "Access-Control-Allow-Methods",
                    "GET, POST, OPTIONS",
                )
                for header_name, header_value in dict(headers or {}).items():
                    self.send_header(str(header_name), str(header_value))
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
            except (BrokenPipeError, ConnectionAbortedError, ConnectionResetError) as e:
                _warn("response_write_disconnected", e, status=status)
                return
            except Exception as e:
                _warn("response_write", e, status=status)
                return

        def _direct_client_ip(self):
            try:
                return str(self.client_address[0] or "").strip()
            except Exception as e:
                _warn("direct_client_ip", e)
                return ""

        def _client_ip(self):
            direct_ip = self._direct_client_ip()
            if not trusted_proxy_networks or not _ip_in_networks(direct_ip, trusted_proxy_networks):
                return direct_ip

            try:
                raw_values = self.headers.get_all("X-Forwarded-For") or []
            except Exception as e:
                _warn("xff_header_read", e)
                raw_values = []

            chain = []
            for raw in raw_values:
                for part in str(raw or "").split(","):
                    value = part.strip()
                    if value:
                        chain.append(value)

            if not chain:
                return direct_ip

            for candidate in reversed(chain):
                if not _ip_in_networks(candidate, trusted_proxy_networks):
                    return candidate

            return direct_ip

        def _is_localhost_client(self):
            try:
                return _is_loopback_ip(self._client_ip())
            except Exception as e:
                _warn("localhost_client_check", e)
                return False

        def _request_api_token(self):
            try:
                hdr = (self.headers.get("X-API-Token") or "").strip()
            except Exception as e:
                _warn("auth_header_read", e)
                hdr = ""
            if hdr:
                return hdr

            try:
                parsed = urlparse(self.path)
                q = parse_qs(parsed.query)
                qtok = (q.get("token") or [""])[0]
            except Exception as e:
                _warn("auth_query_parse", e, path=getattr(self, "path", ""))
                qtok = ""
            return str(qtok or "").strip()

        def _require_mutation_auth(self):
            # Mutating endpoints require an API token when configured; otherwise
            # they fall back to localhost-only protection.
            # token based auth
            if dashboard_token:
                supplied = self._request_api_token()
                if hmac.compare_digest(supplied, dashboard_token):
                    return None

                return {"ok": False, "error": "unauthorized"}

            # fallback localhost restriction
            if self._is_localhost_client():
                return None

            return {"ok": False, "error": "forbidden (localhost only)"}

        def _warn_if_token_unset(self, method, path):
            if dashboard_token:
                return
            try:
                log.warning(
                    "INSECURE_DASHBOARD_API_TOKEN_UNSET method=%s path=%s client_ip=%s",
                    str(method or ""),
                    str(path or ""),
                    self._client_ip(),
                )
            except Exception as e:
                _warn("token_unset_warning", e, method=method, path=path)

        def _rate_limit_mutation(self, parsed_path):
            token_for_bucket = self._request_api_token() if dashboard_token else ""
            destructive = str(parsed_path or "") in _DESTRUCTIVE_ENDPOINT_PATHS
            try:
                decision = rate_limiter.check(
                    token=token_for_bucket,
                    ip=self._client_ip(),
                    destructive=destructive,
                )
            except Exception as e:
                _warn("rate_limit_check", e, path=str(parsed_path or ""))
                return None

            if decision.allowed:
                return None

            retry_after = max(1, int(decision.retry_after_s or 1))
            return (
                {
                    "ok": False,
                    "error": "rate_limit_exceeded",
                    "retry_after_s": retry_after,
                    "limit_per_min": int(decision.limit_per_min or 0),
                },
                {"Retry-After": str(retry_after)},
            )

        # --------------------------------------------------------
        # Core Dispatch
        # --------------------------------------------------------

        def _dispatch(self):
            started = time.time()

            method = str(self.command or "").upper().strip()
            self._response_status = None
            self._response_ok = None
            self._response_output_valid = None
            self._response_streaming = False
            self._request_body_valid = True

            self._normalize_ui_legacy_path()

            parsed = urlparse(self.path)
            key = (method, parsed.path)
            handler_name = self.ROUTES.get(key)
            parsed_for_handler = parsed

            # tolerate trailing slash mismatches
            if not handler_name and parsed.path.endswith("/"):
                key = (method, parsed.path.rstrip("/"))
                handler_name = self.ROUTES.get(key)
                if handler_name:
                    parsed = urlparse(parsed._replace(path=parsed.path.rstrip("/")).geturl())
                    parsed_for_handler = parsed

            if not handler_name:
                for route_path, route_regex, route_names, route_handler in self.TEMPLATE_ROUTES.get(method, []):
                    match = route_regex.match(parsed.path)
                    if not match:
                        continue
                    try:
                        raw_qs = parse_qs(parsed.query, keep_blank_values=True)
                        parsed_for_handler = {
                            str(k): ("" if not v else str(v[0]))
                            for k, v in raw_qs.items()
                        }
                    except Exception as e:
                        _warn("template_query_parse", e, path=parsed.path)
                        parsed_for_handler = {}
                    for name in route_names:
                        parsed_for_handler[str(name)] = unquote(str(match.group(name) or ""))
                    parsed_for_handler["_route_path"] = str(route_path)
                    parsed_for_handler["_path"] = str(parsed.path or "")
                    handler_name = route_handler
                    break

            # no route → static or 404
            if not handler_name:

                if method == "GET":
                    return super().do_GET()

                return self.respond_json(
                    {"ok": False, "error": "unknown_endpoint"},
                    404,
                )

            fn = API_HANDLERS.get(handler_name)

            if not fn:
                return self.respond_json(
                    {
                        "ok": False,
                        "error": f"handler_missing:{handler_name}",
                    },
                    500,
                )

            self._warn_if_token_unset(method, parsed.path)

            if method == "GET" and parsed.path in _storage_required_paths(self._ctx):
                try:
                    from engine.runtime.storage_pool import probe_storage_readiness

                    readiness = probe_storage_readiness(
                        timeout_s=_request_storage_timeout_s(self._ctx),
                        max_age_s=_storage_readiness_cache_s(self._ctx),
                    )
                except Exception as e:
                    return self.respond_json(
                        _storage_unavailable_response(endpoint=parsed.path, error=e),
                        503,
                    )
                if not bool((readiness or {}).get("ok")):
                    return self.respond_json(
                        _storage_unavailable_response(endpoint=parsed.path, readiness=readiness),
                        503,
                    )

            # ----------------------------------------------------
            # mutation protection
            # ----------------------------------------------------

            if method != "GET":

                denied = deny_if_shutdown()
                if denied:
                    return self.respond_json(denied, 503)

                auth = self._require_mutation_auth()
                if auth:
                    return self.respond_json(auth, 403)

                limited = self._rate_limit_mutation(parsed.path)
                if limited:
                    payload, headers = limited
                    return self.respond_json(payload, 429, headers=headers)

            try:

                body = None

                if method != "GET":
                    body = self._read_json_body() or {}

                    if isinstance(body, dict):

                        if body.get("__body_error__") == "body_too_large":
                            self._request_body_valid = False
                            return self.respond_json(
                                {
                                    "ok": False,
                                    "error": "body_too_large",
                                    "max_bytes": _MAX_JSON_BODY_BYTES,
                                    "content_length": body.get(
                                        "__body_bytes__"
                                    ),
                                },
                                413,
                            )

                        if body.get("__body_error__") == "invalid_json":
                            self._request_body_valid = False
                            return self.respond_json(
                                {"ok": False, "error": "invalid_json"},
                                400,
                            )

                        if body.get("__body_error__") == "body_read_failed":
                            self._request_body_valid = False
                            return self.respond_json(
                                {"ok": False, "error": "body_read_failed"},
                                400,
                            )

                try:
                    from engine.runtime.storage_pool import storage_acquire_timeout_override

                    storage_timeout_ctx = storage_acquire_timeout_override(
                        _request_storage_timeout_s(self._ctx)
                    )
                except Exception:
                    storage_timeout_ctx = nullcontext()

                with storage_timeout_ctx:
                    result = _call_handler(
                        fn,
                        method=method,
                        parsed=parsed_for_handler,
                        body=body,
                        handler_ctx=self._ctx,
                    )

                if result is None:
                    result = {
                        "ok": False,
                        "error": "empty_response",
                        "meta": {"handler": str(handler_name)},
                    }

                if isinstance(result, StreamingResponse):
                    self._record_response_observation(
                        status=int(result.status),
                        ok=True,
                        output_valid=callable(result.stream_fn),
                        streaming=True,
                    )
                    self.send_response(int(result.status))

                    headers = dict(result.headers or {})
                    if "Content-Type" not in headers:
                        headers["Content-Type"] = "application/octet-stream"

                    for k, v in headers.items():
                        self.send_header(str(k), str(v))

                    self.end_headers()
                    try:
                        if callable(result.stream_fn):
                            return result.stream_fn(self)
                    except Exception as e:
                        _warn("streaming_response", e, handler=handler_name or "")
                        return
                    return

                return self.respond_json(result)

            except (BrokenPipeError, ConnectionAbortedError, ConnectionResetError) as e:
                _warn("handler_disconnected", e, handler=handler_name or "")
                return

            except Exception as e:
                if _is_storage_acquisition_exception(e):
                    log.warning(
                        "http_transport_handler_storage_unavailable handler=%s method=%s path=%s error=%s",
                        handler_name or "",
                        method,
                        getattr(self, "path", ""),
                        f"{type(e).__name__}: {e}",
                    )
                    return self.respond_json(
                        _storage_unavailable_response(endpoint=parsed.path, error=e),
                        503,
                    )

                _warn(
                    "handler_exception",
                    e,
                    handler=handler_name or "",
                    method=method,
                    path=getattr(self, "path", ""),
                )

                return self.respond_json(
                    {
                        "ok": False,
                        "error": "internal_server_error",
                        "detail": f"{type(e).__name__}: {e}",
                    },
                    500,
                )
            finally:
                elapsed_ms = int((time.time() - started) * 1000)
                if elapsed_ms >= 2000:
                    try:
                        print(
                            "HTTP_SLOW_REQUEST "
                            f"method={method} "
                            f"path={parsed.path if 'parsed' in locals() else self.path} "
                            f"handler={handler_name or ''} "
                            f"elapsed_ms={elapsed_ms}",
                            flush=True,
                        )
                    except Exception as e:
                        _warn(
                            "slow_request_log",
                            e,
                            method=method,
                            path=parsed.path if 'parsed' in locals() else getattr(self, "path", ""),
                            handler=handler_name or "",
                            elapsed_ms=elapsed_ms,
                        )
                try:
                    if not _storage_known_unavailable():
                        status_tag = (
                            int(self._response_status)
                            if self._response_status is not None
                            else 0
                        )
                        route_path = parsed.path if "parsed" in locals() else getattr(self, "path", "")
                        common_tags = {
                            "method": method,
                            "path": str(route_path or ""),
                            "handler": str(handler_name or ""),
                            "status": status_tag,
                            "body_valid": int(bool(self._request_body_valid)),
                            "output_valid": int(bool(self._response_output_valid)),
                            "streaming": int(bool(self._response_streaming)),
                        }
                        emit_counter(
                            "http_request_total",
                            1,
                            component="engine.api.http_transport",
                            extra_tags=common_tags,
                        )
                        if self._response_ok is False or not bool(self._request_body_valid):
                            emit_counter(
                                "http_request_failure_total",
                                1,
                                component="engine.api.http_transport",
                                extra_tags=dict(
                                    common_tags,
                                    ok=(
                                        ""
                                        if self._response_ok is None
                                        else int(bool(self._response_ok))
                                    ),
                                ),
                            )
                        if (self._response_output_valid is False) or (not bool(self._request_body_valid)):
                            emit_counter(
                                "http_request_validation_total",
                                1,
                                component="engine.api.http_transport",
                                extra_tags=dict(common_tags, validation="failed"),
                            )
                        emit_timing(
                            "http_request_latency_ms",
                            elapsed_ms,
                            component="engine.api.http_transport",
                            extra_tags=common_tags,
                        )
                except Exception as e:
                    _warn(
                        "request_metrics_emit",
                        e,
                        method=method,
                        path=parsed.path if 'parsed' in locals() else getattr(self, "path", ""),
                        handler=handler_name or "",
                    )

        # --------------------------------------------------------
        # HTTP verbs
        # --------------------------------------------------------

        def do_GET(self):
            self._dispatch()

        def do_POST(self):
            self._dispatch()

        def do_OPTIONS(self):
            try:
                req_origin = (self.headers.get("Origin") or "").strip()
            except Exception as e:
                _warn("options_origin_read", e)
                req_origin = ""

            allow_origin = "http://127.0.0.1:8000"
            if req_origin in ("http://127.0.0.1:8000", "http://localhost:8000"):
                allow_origin = req_origin

            self.send_response(204)
            self.send_header("Access-Control-Allow-Origin", allow_origin)
            self.send_header("Vary", "Origin")
            self.send_header(
                "Access-Control-Allow-Headers",
                "Content-Type, X-API-Token",
            )
            self.send_header(
                "Access-Control-Allow-Methods",
                "GET, POST, OPTIONS",
            )
            self.send_header("Cache-Control", "no-store")
            self.end_headers()

    return Handler



def run_http_server(host, port, handler_cls):

    class _ReusableThreadingHTTPServer(ThreadingHTTPServer):
        allow_reuse_address = True
        daemon_threads = True

    try:
        httpd = _ReusableThreadingHTTPServer((host, int(port)), handler_cls)
    except Exception as e:
        print(
            f"RUN_HTTP_SERVER_BIND_FATAL host={host} port={int(port)} error={e}",
            flush=True,
        )
        traceback.print_exc()
        raise RuntimeError(
            f"run_http_server_bind_failed host={host} port={int(port)} error={e}"
        ) from e

    return httpd
