from __future__ import annotations

from itertools import pairwise

import pytest

from sec_lakehouse.sources.form13f.datasets import (
    DATASETS_BY_KEY,
    FORM13F_DATASETS,
    FORM13F_PARTITIONS,
    dataset_key,
)


@pytest.mark.parametrize(
    ("zip_name", "key"),
    [
        ("2013q2_form13f.zip", "2013q2"),
        ("2023q4_form13f.zip", "2023q4"),
        ("01jan2024-29feb2024_form13f.zip", "2024-01_2024-02"),  # the two-month transition set
        ("01mar2024-31may2024_form13f.zip", "2024-03_2024-05"),
        ("01dec2025-28feb2026_form13f.zip", "2025-12_2026-02"),  # crosses a year end
    ],
)
def test_dataset_key(zip_name: str, key: str) -> None:
    assert dataset_key(zip_name) == key


@pytest.mark.parametrize(
    "bad",
    [
        "2013q5_form13f.zip",
        "2013Q2_form13f.zip.bak",
        "form13f.zip",
        "01foo2024-29feb2024_form13f.zip",
        "31may2024-01mar2024_form13f.zip",
    ],
)
def test_dataset_key_rejects_unknown_names(bad: str) -> None:
    with pytest.raises(ValueError):
        dataset_key(bad)


def test_partition_list_covers_2013q2_to_aug_2026() -> None:
    keys = FORM13F_PARTITIONS.get_partition_keys()
    assert len(keys) == 54  # as listed on the SEC page on 2026-11-06
    assert keys[0] == "2013q2"
    assert keys[-1] == "2026-06_2026-08"
    assert "2013q1" not in keys


def test_keys_are_unique_and_sort_in_time_order() -> None:
    keys = [d.key for d in FORM13F_DATASETS]
    assert len(set(keys)) == len(keys)
    assert keys == sorted(keys)


def test_windows_from_2024_are_contiguous() -> None:
    windows = [d.key for d in FORM13F_DATASETS if "_" in d.key]
    for earlier, later in pairwise(windows):
        end_year, end_month = map(int, earlier.split("_")[1].split("-"))
        start_year, start_month = map(int, later.split("_")[0].split("-"))
        assert (start_year * 12 + start_month) - (end_year * 12 + end_month) == 1


def test_urls_are_sec_https_and_match_zip_names() -> None:
    for dataset in FORM13F_DATASETS:
        assert dataset.url.startswith("https://www.sec.gov/files/")
        assert dataset.url.endswith("/" + dataset.zip_name)
    assert DATASETS_BY_KEY["2026-06_2026-08"].url == (
        "https://www.sec.gov/files/datastandardsinnovation/data/form-13f-data-sets/"
        "01jun2026-31aug2026_form13f.zip"
    )
