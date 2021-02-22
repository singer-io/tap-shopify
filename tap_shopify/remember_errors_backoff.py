import os
import logging
import math
import time
import random


class RememberErrorsBackoff:
    def __init__(self):
        self.modulo = 6
        self.errors_in_minutes = [0] * self.modulo
        self.last_idx = None
        self.sleep_steps = float(os.getenv("BACKOFF_STEPS", 0.5))
        self.max_sleep = float(os.getenv("BACKOFF_MAX_SECONDS", 120))

    def on_error(self, details):
        idx = self._get_idx()
        if idx != self.last_idx:
            self.last_idx = idx
            self.errors_in_minutes[idx] = 0

        self.errors_in_minutes[idx] += 1

    def _get_idx(self) -> int:
        return int(math.ceil(time.time() / 10) % self.modulo)

    def on_success(self, details):
        idx = self._get_idx()
        logging.info("success {}".format(idx))
        if idx != self.last_idx:
            self.last_idx = idx
            self.errors_in_minutes[idx] = 0

    def get_yield(self):
        while True:
            errors_last_minutes = sum(self.errors_in_minutes)
            sleep_for = math.ceil(self.sleep_steps * errors_last_minutes) + 1
            logging.info("had {} errors within the last minute, max sleep {}".format(errors_last_minutes, sleep_for))

            sleep_for = min(sleep_for, self.max_sleep)
            yield random.randint(1,sleep_for)