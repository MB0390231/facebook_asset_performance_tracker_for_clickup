import logging


loggers_dict = {}


def get_logger(name=None, console_level=logging.INFO):
    global loggers_dict

    if name is None:
        name = "BaseLogger"

    if name in loggers_dict:
        logger = loggers_dict[name]
    else:
        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG)

        file_handler = logging.FileHandler(f"logs/runtime.log", "a")
        file_handler.setFormatter(logging.Formatter("%(asctime)s : %(levelname)s : %(name)s : %(message)s"))
        logger.addHandler(file_handler)

        console_handler = logging.StreamHandler()
        console_handler.setLevel(console_level)
        console_handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(console_handler)

        loggers_dict[name] = logger

    return logger
