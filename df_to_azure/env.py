import os


def get_env(name: str):
    """
    Return an environment variable with backward-compatible casing.

    Existing names keep priority. For example, ``subscription_id`` is preferred over
    ``SUBSCRIPTION_ID`` to avoid changing existing deployments, but both are accepted.
    """
    value = os.environ.get(name)
    if value is not None:
        return value

    upper_name = name.upper()
    if upper_name != name:
        value = os.environ.get(upper_name)
        if value is not None:
            return value

    lower_name = name.lower()
    if lower_name != name:
        return os.environ.get(lower_name)

    return None
