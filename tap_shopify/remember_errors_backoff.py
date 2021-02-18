import os
import logging


class RememberErrorsBackoff:
    def __init__(self):
        self.errors = 0
        self.sleep_steps = float(os.getenv("BACKOFF_STEPS", 0.1))
        self.max_sleep = float(os.getenv("BACKOFF_MAX_SECONDS", 10))
        self.success_without_error = 0

    def on_error(self, details):
        self.success_without_error = 0
        logging.info("Received 429 -- sleeping for %s seconds", details['wait'])
        self.errors += 1

    def on_success(self, details):
        self.success_without_error += 1
        self.errors -= self.success_without_error
        self.errors = max(0, self.errors)

    def get_yield(self):
        while True:
            errors = max(self.errors, 1)
            sleep_time = errors * self.sleep_steps
            yield min(self.max_sleep, sleep_time)