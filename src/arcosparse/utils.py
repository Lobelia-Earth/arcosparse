import calendar
import concurrent.futures
import functools
from datetime import datetime
from typing import Any, Callable, Optional, Sequence, TypeVar, Union

from dateutil import parser as dateutil_parser
from dateutil.parser import ParserError
from tqdm import tqdm

from arcosparse.logger import logger

_T = TypeVar("_T")


# From: https://stackoverflow.com/a/46144596/20983727
def run_concurrently(
    func: Callable[..., _T],
    function_arguments: Sequence[tuple[Any, ...]],
    max_concurrent_requests: int,
    tdqm_bar_configuration: dict = {},
) -> list[_T]:
    out = []
    with tqdm(
        total=len(function_arguments),
        **tdqm_bar_configuration,
    ) as pbar:
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=max_concurrent_requests
        ) as executor:
            future_to_url = (
                executor.submit(func, *function_argument)
                for function_argument in function_arguments
            )
            for future in concurrent.futures.as_completed(future_to_url):
                data = future.result()
                out.append(data)
                pbar.update(1)
    return out


def date_to_timestamp(date: Union[str, float], time_unit: str) -> float:
    """
    Warning: If the input date is a float or int, it won't be converted to the input time unit.
    """  # noqa: E501
    if isinstance(date, float) or isinstance(date, int):
        return date
    dt = datetime_parser(date)
    conversion_factor = 1000 if "milliseconds" in time_unit else 1
    return calendar.timegm(dt.timetuple()) * conversion_factor


def datetime_parser(date: str) -> datetime:
    try:
        return dateutil_parser.parse(date)
    except ParserError:
        logger.error(f"Failed to parse date: {date}")
        raise


def deprecated(replacement: Optional[Callable] = None) -> Callable:
    def deco(func: Callable):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            message = f"'{func.__name__}' is deprecated"
            if replacement:
                message += f", use '{replacement.__name__}' instead."
                logger.warning(message)
                return replacement(*args, **kwargs)
            else:
                logger.warning(message + ".")
                return func(*args, **kwargs)

        return wrapper

    return deco
