import functools
import logging
import math
import time
from datetime import timedelta


def timer(func):
    """Decorator to track running time"""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        t0 = time.time()
        result = func(*args, **kwargs)
        t = time.time() - t0
        readable_time = str(timedelta(seconds=math.floor(t)))
        logging.info(f"Function {func.__name__!r} executed in {readable_time}")
        return result

    return wrapper

def set_env_value(production, dev):
    """Change variable value based on execution environment"""
    from airflow.models import Variable # import here to avoid error when using other module

    execution_environment = Variable.get('execution_environment', default_var="dev")
    return production if execution_environment == 'production' else dev
