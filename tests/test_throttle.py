import unittest

from app.api_client import RateLimiter


class FakeClock:
    def __init__(self) -> None:
        self.t = 0.0

    def now(self) -> float:
        return self.t

    def sleep(self, seconds: float) -> None:
        self.t += seconds


class ThrottleTests(unittest.TestCase):
    def test_rate_limiter_does_not_exceed_80_per_minute(self) -> None:
        clock = FakeClock()
        limiter = RateLimiter(
            max_per_minute=80,
            min_interval_seconds=0.75,
            now_fn=clock.now,
            sleep_fn=clock.sleep,
        )

        start = clock.now()
        for _ in range(81):
            limiter.wait()
        elapsed = clock.now() - start

        # 81 requests must take at least a minute at the configured safety cap.
        self.assertGreaterEqual(elapsed, 60.0)


if __name__ == "__main__":
    unittest.main()
