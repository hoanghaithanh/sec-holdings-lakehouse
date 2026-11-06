"""The SEC Form 13F data sets, one Dagster partition each (ADR-002).

SEC publishes one zip per period: calendar quarters up to 2023 (`2013q2_form13f.zip`), then
three-month filing windows from 2024 (`01mar2024-31may2024_form13f.zip`). Each zip becomes one
partition with a key that sorts in time order: `2013q2` ... `2023q4`, then `YYYY-MM_YYYY-MM`.

The URL of each zip is kept exactly as SEC links it, because the folder is not the same for
every file. To add a newly published data set, append its URL to `FORM13F_ZIP_URLS` (see the
runbook). The list was copied from the SEC data sets page on 2026-11-06.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from urllib.parse import urlsplit

from dagster import StaticPartitionsDefinition

DATASETS_PAGE = "https://www.sec.gov/data-research/sec-markets-data/form-13f-data-sets"

_BASE = "https://www.sec.gov/files/structureddata/data/form-13f-data-sets"
_BASE_2026 = "https://www.sec.gov/files/datastandardsinnovation/data/form-13f-data-sets"

_QUARTERS = [f"{year}q{q}" for year in range(2013, 2024) for q in range(1, 5)][1:]  # from 2013q2
_WINDOWS_2024_ON = [
    "01jan2024-29feb2024",
    "01mar2024-31may2024",
    "01jun2024-31aug2024",
    "01sep2024-30nov2024",
    "01dec2024-28feb2025",
    "01mar2025-31may2025",
    "01jun2025-31aug2025",
    "01sep2025-30nov2025",
    "01dec2025-28feb2026",
    "01mar2026-31may2026",
]

FORM13F_ZIP_URLS: tuple[str, ...] = (
    *(f"{_BASE}/{name}_form13f.zip" for name in [*_QUARTERS, *_WINDOWS_2024_ON]),
    f"{_BASE_2026}/01jun2026-31aug2026_form13f.zip",
)

_QUARTER_NAME = re.compile(r"(?P<year>\d{4})q(?P<q>[1-4])_form13f\.zip")
_WINDOW_NAME = re.compile(r"(?P<start>\d{2}[a-z]{3}\d{4})-(?P<end>\d{2}[a-z]{3}\d{4})_form13f\.zip")


@dataclass(frozen=True)
class Form13fDataset:
    key: str
    zip_name: str
    url: str


def _parse_day(text: str) -> date:
    return datetime.strptime(text, "%d%b%Y").date()  # SEC uses lower-case English month names


def dataset_key(zip_name: str) -> str:
    """Partition key for an SEC zip name: `2013q2`, or `2024-03_2024-05` for a window."""
    if match := _QUARTER_NAME.fullmatch(zip_name):
        return f"{match['year']}q{match['q']}"
    if match := _WINDOW_NAME.fullmatch(zip_name):
        try:
            start, end = _parse_day(match["start"]), _parse_day(match["end"])
        except ValueError as exc:
            raise ValueError(f"bad date in 13F data set name {zip_name!r}") from exc
        if end < start:
            raise ValueError(f"13F data set window ends before it starts: {zip_name!r}")
        return f"{start:%Y-%m}_{end:%Y-%m}"
    raise ValueError(f"not a 13F data set zip name: {zip_name!r}")


def dataset_from_url(url: str) -> Form13fDataset:
    zip_name = urlsplit(url).path.rsplit("/", 1)[-1]
    return Form13fDataset(key=dataset_key(zip_name), zip_name=zip_name, url=url)


FORM13F_DATASETS: tuple[Form13fDataset, ...] = tuple(dataset_from_url(u) for u in FORM13F_ZIP_URLS)
DATASETS_BY_KEY: dict[str, Form13fDataset] = {d.key: d for d in FORM13F_DATASETS}

FORM13F_PARTITIONS = StaticPartitionsDefinition([d.key for d in FORM13F_DATASETS])
