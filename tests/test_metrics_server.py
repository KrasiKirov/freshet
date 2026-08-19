"""A metrics port that is already taken must not stop the worker.

Regression: `make embedder` died with OSError [Errno 48] when a second copy was
started, because the Prometheus server bound before any work began. Metrics are
observability; the embedder's job is indexing, and it must keep doing it.
"""
import socket

from freshet.pipeline import metrics


def _free_port() -> int:
    s = socket.socket()
    s.bind(("", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _http_status(port: int) -> bytes:
    with socket.create_connection(("127.0.0.1", port), timeout=5) as c:
        c.sendall(b"GET /metrics HTTP/1.0\r\n\r\n")
        return c.recv(64)


def test_second_bind_on_the_same_port_is_logged_and_ignored(caplog):
    # Reproduces the real collision: two workers, same metrics port. Binding a
    # loopback-only socket would NOT collide, since prometheus_client listens on
    # 0.0.0.0 — the first server has to be a real one.
    port = _free_port()
    metrics.start_metrics_server(port)
    assert b"200" in _http_status(port), "first server should be serving"

    with caplog.at_level("WARNING"):
        metrics.start_metrics_server(port)      # must not raise
    assert "metrics server disabled" in caplog.text
    assert str(port) in caplog.text

    # the original server is untouched and still serving
    assert b"200" in _http_status(port)


def test_port_zero_starts_nothing(caplog):
    with caplog.at_level("WARNING"):
        metrics.start_metrics_server(0)
    assert caplog.text == ""      # disabled deliberately: not a failure to report
