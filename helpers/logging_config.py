import logging
from helpers import helpers


class BaseLogger:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.DEBUG)

        file_handler = logging.FileHandler(f"logs/{helpers.current_date()}.log", "a")
        file_handler.setFormatter(logging.Formatter("%(asctime)s : %(levelname)s : %(name)s : %(message)s"))
        self.logger.addHandler(file_handler)

        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(logging.Formatter("%(message)s"))
        self.logger.addHandler(console_handler)
