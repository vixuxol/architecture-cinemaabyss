import os
from typing import Callable, Optional, Union


class _NoValue:
    pass


NO_VALUE = _NoValue()


def get_str_var(var_name: str, default: Union[str, _NoValue, None] = NO_VALUE) -> Optional[str]:
    value = _get_var(var_name, default)
    return value if value is None else str(value)


def get_int_var(var_name: str, default: Union[int, _NoValue, None] = NO_VALUE) -> Optional[int]:
    value = _get_var(var_name, default)
    return value if value is None else int(value)


def get_bool_var(var_name: str, default: Union[bool, _NoValue, None] = NO_VALUE) -> Optional[bool]:
    value = _get_var(var_name, default)
    return value if value is None else bool(value)


def parse_env_var(var_name: str, default, raise_error: bool = True, parser: Optional[Callable] = None):
    if parser is None:
        if default is not None:
            parser = default.__class__

    value = os.environ.get(var_name, None)
    if value is None:
        if default is None and raise_error:
            raise Exception()
        return default
    if parser is None:
        raise Exception()
    value = parser(value)
    return value


def _get_var(var_name: str, default: Union[str, bool, _NoValue, None]):
    value = os.environ.get(var_name, None)
    if value is None:
        if default is NO_VALUE:
            raise Exception(f"No value provided for '{var_name}'")
        return default
    return value
