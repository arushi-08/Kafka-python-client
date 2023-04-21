from inspect import isbuiltin, isclass, isfunction, ismethod

# noinspection PyUnresolvedReferences
def get_call_repr(func, *args, **kwargs) -> str:
    """Return the string representation of the function call.

    :param func: A callable (e.g. function, method).
    :type func: callable
    :param args: Positional arguments for the callable.
    :param kwargs: Keyword arguments for the callable.
    :return: String representation of the function call.
    :rtype: str
    """
    # Functions, builtins and methods
    if ismethod(func) or isfunction(func) or isbuiltin(func):
        func_repr = f"{func.__module__}.{func.__qualname__}"
    # A callable class instance
    elif not isclass(func) and hasattr(func, "__call__"):
        func_repr = f"{func.__module__}.{func.__class__.__name__}"
    else:
        func_repr = repr(func)

    args_reprs = [repr(arg) for arg in args]
    kwargs_reprs = [k + "=" + repr(v) for k, v in sorted(kwargs.items())]
    return f'{func_repr}({", ".join(args_reprs + kwargs_reprs)})'
