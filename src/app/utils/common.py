import os


def get_env_var(name: str) -> str:
    """Return the value of a required environment variable.

    This helper reads an environment variable and raises an error if it is
    not set or is an empty string. Use it to enforce required configuration
    at startup.

    Args:
        name (str): Name of the environment variable to read.

    Returns:
        str: The non-empty value of the requested environment variable.

    Raises:
        EnvironmentError: If the environment variable is not set or empty.
    """
    value = os.environ.get(name)
    if not value:
        raise EnvironmentError(f"{name} environment variable not set")
    return value
