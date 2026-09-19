"""Test-only proxy: drop a real producer success AFTER fully reading its response.

In drop mode GETs fail without reaching inventory, so the worker cannot resolve
the pending order before the restart checkpoint. No production test hooks.
"""
import http.client
import json
import socket
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

lock = threading.Lock()
state = {"mode": "drop", "dropped": [], "forwarded": []}


def read_body(headers, stream):
    if headers.get("Transfer-Encoding", "").lower() == "chunked":
        chunks = []
        while True:
            size = int(stream.readline().split(b";", 1)[0], 16)
            if size == 0:
                while stream.readline() not in (b"\r\n", b""):
                    pass
                return b"".join(chunks)
            chunks.append(stream.read(size))
            assert stream.read(2) == b"\r\n"
    return stream.read(int(headers.get("Content-Length", 0)))


class Proxy(BaseHTTPRequestHandler):
    def reply(self, status, body, content_type="application/json"):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        self.route()

    def do_GET(self):
        self.route()

    def route(self):
        body = read_body(self.headers, self.rfile)
        if self.path == "/__control":
            with lock:
                if self.command == "POST":
                    mode = json.loads(body)["mode"]
                    assert mode in ("drop", "pass")
                    state["mode"] = mode
                result = json.dumps(state).encode()
            self.reply(200, result)
            return
        with lock:
            dropping = state["mode"] == "drop"
        if dropping and self.command == "GET":
            self.reply(503, b'{}')
            return
        upstream = http.client.HTTPConnection("inventory-service", 8081, timeout=5)
        try:
            headers = {key: value for key, value in self.headers.items()
                       if key.lower() not in ("host", "connection", "content-length", "transfer-encoding", "trailer")}
            upstream.request(self.command, self.path, body, headers)
            response = upstream.getresponse()
            payload = response.read()
            event = {"method": self.command, "path": self.path, "status": response.status}
            if self.command == "POST":
                event["orderId"] = json.loads(body)["orderId"]
            drop = (dropping and self.command == "POST"
                    and self.path == "/api/v1/reservations" and response.status in (200, 201))
            if drop:
                reservation = json.loads(payload)
                assert reservation["status"] == "RESERVED"
                assert reservation["orderId"] == event["orderId"]
            with lock:
                state["forwarded"].append(event)
                if drop:
                    state["dropped"].append(event)
            print(json.dumps({"dropped": drop, **event}), flush=True)
            if drop:
                self.close_connection = True
                self.connection.shutdown(socket.SHUT_RDWR)
                self.connection.close()
            else:
                self.reply(response.status, payload, response.getheader("Content-Type", "application/json"))
        finally:
            upstream.close()


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8082), Proxy).serve_forever()
