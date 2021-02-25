"""Functions to generate metada."""
from typing import Dict

from oiflib._helper import indicator_lookup, theme_lookup


def _generate_metadata(
    metadata: Dict[str, str],
    trend_description: str,
) -> str:
    """# TODO [summary].

    Args:
        metadata (Dict[str, str]): #TODO [description]
        trend_description (str): #TODO [description]

    Returns:
        str: #TODO [description]
    """
    lst = [
        "---",
        "\n".join([f"{key}: {value}" for key, value in metadata.items()]),
        "---",
        f"**Trend description**: {trend_description}",
    ]

    markdown = "\n".join(lst)

    return markdown


def _set_meta_file_name(
    theme: str,
    indicator: str,
) -> str:
    """#TODO [summary].

    Args:
        theme (str): #TODO [description]
        indicator (str): #TODO [description]

    Returns:
        str: #TODO [description]
    """
    return f"{theme_lookup.get(theme)}-{indicator_lookup.get(indicator)}-1.md"


def _write_to_md(
    markdown: str,
    repo: str,
    folder: str,
    file_name: str,
) -> None:
    """#TODO [summary].

    Args:
        markdown (str): #TODO [description]
        repo (str): #TODO [description]
        folder (str): #TODO [description]
        file_name (str): #TODO [description]
    """
    with open(f"{repo}/{folder}/{file_name}", "w") as file:
        file.write(markdown)


def generate_markdown(
    metadata: Dict[str, str],
    trend_description: str,
    theme: str,
    indicator: str,
    repo: str = "OIF-Dashboard-Data",
    folder: str = "meta",
) -> None:
    """#TODO [summary].

    Args:
        metadata (Dict[str, str]): #TODO [description]
        trend_description (str): #TODO [description]
        theme (str): #TODO [description]
        indicator (str): #TODO [description]
        repo (str): #TODO [description]. Defaults to "OIF-Dashboard-Data".
        folder (str): #TODO [description]. Defaults to "meta".
    """
    _markdown: str = _generate_metadata(
        metadata=metadata,
        trend_description=trend_description,
    )

    _file_name: str = _set_meta_file_name(
        theme=theme,
        indicator=indicator,
    )

    _write_to_md(
        markdown=_markdown,
        repo=repo,
        folder=folder,
        file_name=_file_name,
    )
