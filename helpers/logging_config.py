import logging


def get_logger(name, level=logging.DEBUG):
    logger = logging.getLogger(name)
    logger.setLevel(level)
    formatter = logging.Formatter("%(asctime)s : %(levelname)s : %(message)s")
    console_formatter = logging.Formatter("%(message)s")
    file_handler = logging.FileHandler(f"/Users/mb0390231/Documents/RGM/rgm-northstar-clickup/logs/{name}.log", "w")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    return logger
