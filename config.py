from attrs import define


@define(frozen=True, slots=True)
class RetryPolicy:
    max_retries: int = 4
    base_delay: float = 0.5
    jitter_low: float = 0.0
    jitter_high: float = 0.2
    retry_statuses: tuple[int, ...] = (429, 500, 502, 503, 504)


@define(frozen=True, slots=True)
class HttpConfig:
    timeout: float = 30.0
    concurrency: int = 10
    headers: dict = {
        "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 "
        "Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/139.0.0.0 Mobile Safari/537.36"
    }
