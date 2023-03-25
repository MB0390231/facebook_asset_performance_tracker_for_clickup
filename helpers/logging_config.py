import logging
import datetime


def current_date():
    return datetime.datetime.now().strftime("%Y-%m-%d")


class BaseLogger:
    def __init__(self, name=None, console_level=logging.INFO):
        if name is None:
            name = self.__class__.__name__
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)

        file_handler = logging.FileHandler(f"logs/{current_date()}.log", "a")
        file_handler.setFormatter(logging.Formatter("%(asctime)s : %(levelname)s : %(name)s : %(message)s"))
        self.logger.addHandler(file_handler)

        console_handler = logging.StreamHandler()
        console_handler.setLevel(console_level)
        console_handler.setFormatter(logging.Formatter("%(message)s"))
        self.logger.addHandler(console_handler)
