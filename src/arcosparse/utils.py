import concurrent.futures
from datetime import datetime
from typing import Any, Callable, Sequence, TypeVar, Union

_T = TypeVar("_T")


# From: https://stackoverflow.com/a/46144596/20983727
def run_concurrently(
    func: Callable[..., _T],
    function_arguments: Sequence[tuple[Any, ...]],
    max_concurrent_requests: int,
    tdqm_bar_configuration: dict = {},
) -> list[_T]:
    # TODO: add support to tdqm somehow
    out = []
    # with tqdm(
    #     total=len(function_arguments),
    #     **tdqm_bar_configuration,
    # ) as pbar:
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
            # pbar.update(1)
    return out


def date_to_timestamp(date: Union[str, float]) -> float:
    if isinstance(date, float) or isinstance(date, int):
        return date
    return datetime.strptime(date, "%Y-%m-%dT%H:%M:%SZ").timestamp()
