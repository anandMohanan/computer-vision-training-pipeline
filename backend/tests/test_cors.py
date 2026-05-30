import asyncio
import unittest

from backend.main import app


async def _request(method: str, path: str, headers: dict[str, str]) -> tuple[int, dict[str, str]]:
    status_code = 0
    response_headers: dict[str, str] = {}
    events = []

    scope = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "method": method,
        "scheme": "http",
        "path": path,
        "raw_path": path.encode("ascii"),
        "query_string": b"",
        "headers": [(name.lower().encode("ascii"), value.encode("ascii")) for name, value in headers.items()],
        "client": ("testclient", 50000),
        "server": ("testserver", 80),
    }

    async def receive():
        if events:
            return events.pop(0)
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message):
        nonlocal status_code, response_headers
        if message["type"] == "http.response.start":
            status_code = message["status"]
            response_headers = {name.decode("latin-1"): value.decode("latin-1") for name, value in message["headers"]}

    await app(scope, receive, send)
    return status_code, response_headers


class CorsTests(unittest.TestCase):
    def test_label_studio_can_preflight_sample_images(self) -> None:
        status_code, headers = asyncio.run(
            _request(
                "OPTIONS",
            "/samples/2026/05/31/edge-cam-01/sample.jpg",
                {
                    "Origin": "http://localhost:8080",
                    "Access-Control-Request-Method": "GET",
                },
            )
        )

        self.assertEqual(status_code, 200)
        self.assertEqual(headers["access-control-allow-origin"], "http://localhost:8080")


if __name__ == "__main__":
    unittest.main()
