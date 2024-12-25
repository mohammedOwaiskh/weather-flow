import configparser


def read_application_config(
    key: str = None, section: str = "APP_DEFAULT"
) -> str | list[tuple]:
    """Reads configuration from an application configuration file.

    Retrieves configuration values from a specified section of the application configuration file, with flexibility to fetch either a specific key or all items in a section.

    Args:
        key: Optional configuration key to retrieve. If None, returns all items in the section.
        section: Configuration section to read from, defaults to "APP_DEFAULT".

    Returns:
        str or list[tuple]: Value of the specified key, or list of all key-value tuples in the section.

    Examples:
        >>> read_application_config('database_host')
        'localhost'
        >>> read_application_config(section='database')
        [('host', 'localhost'), ('port', '5432')]
    """
    config = configparser.ConfigParser()
    config.read("data-processor\\config\\application.conf")
    return config.items(section) if key is None else config[section][key]


def get_aws_config(key: str) -> str:
    """Retrieves AWS configuration values from the application configuration.

    Fetches a specific configuration value from the 'AWS_CONFIG' section.

    Args:
        key: The specific AWS configuration key to retrieve.

    Returns:
        str: The value associated with the specified AWS configuration key.

    Examples:
        >>> get_aws_config('access_key')
        'your-access-key-here'
    """
    return read_application_config(key, section="AWS_CONFIG")
