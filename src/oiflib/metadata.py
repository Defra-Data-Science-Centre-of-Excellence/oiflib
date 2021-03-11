"""Functions to generate metada."""
from typing import Dict, Optional, Union

from IPython.display import Markdown, display

from oiflib._helper import indicator_lookup, theme_lookup


class MetaData(object):
    """# TODO [summary]."""

    theme: str
    indicator: str
    metadata: Optional[Dict[str, Union[str, Dict[str, str]]]]
    trend_description: Optional[str]
    notes: Optional[str]

    def __init__(
        self,  # noqa: ANN101 - For instance methods, omit type for "self"
        theme: str,
        indicator: str,
        metadata: Optional[Dict[str, Union[str, Dict[str, str]]]] = None,
        trend_description: Optional[str] = None,
        notes: Optional[str] = None,
    ) -> None:
        """# TODO [summary].

        Args:
            theme (str):# TODO [description]
            indicator (str):# TODO [description]
            metadata (Optional[Dict[str, Union[str, Dict[str, str]]]], optional):# TODO
                # TODO [description]. Defaults to None.
            trend_description (Optional[str], optional):# TODO [description]. Defaults
                # TODO to None.
            notes (Optional[str], optional): [description].# TODO Defaults to None.
        """
        self.theme = theme
        self.indicator = indicator
        self.metadata = metadata
        self.trend_description = (
            "**Trend description:** " + trend_description if trend_description else None
        )
        self.notes = "**Notes:** " + notes if notes else None

    def _set_file_name(
        self,  # noqa: ANN101 - For instance methods, omit type for "self"
    ) -> str:
        """# TODO [summary]."""
        return f"{getattr(theme_lookup, self.theme)}-{getattr(indicator_lookup, self.indicator)}-1.md"  # noqa: B950 - breaking this line would reduce readability

    def to_markdown(
        self,  # noqa: ANN101 - For instance methods, omit type for "self"
        to_display: bool = True,
        to_file: bool = False,
        repo: str = "OIF-Dashboard-Data",
        folder: str = "meta",
        file_name: Optional[str] = None,
    ) -> None:
        """# TODO [summary].

        Args:
            to_display (bool): [description]. Defaults to True.
            to_file (bool): [description]. Defaults to False.
            repo (str): [description]. Defaults to "OIF-Dashboard-Data".
            folder (str): [description]. Defaults to "meta".
            file_name (Optional[str]): [description]. Defaults to None.
        """
        lst = []

        lst.extend(
            [
                "---",
                "\n".join([f"{key}: {value}" for key, value in self.metadata.items()]),
                "---",
            ],
        ) if self.metadata else None

        lst.extend(
            [
                f"**Trend description:** {self.trend_description}",
            ],
        ) if self.trend_description else None

        lst.extend(
            [
                f"**Notes:** {self.notes}",
            ],
        ) if self.notes else None

        markdown = "\n".join(lst)

        if to_display:
            display(Markdown(markdown))

        if to_file:
            if not file_name:
                file_name = self._set_file_name()

            with open(f"{repo}/{folder}/{file_name}", "w") as file:
                file.write(markdown)
