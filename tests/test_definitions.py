from sec_lakehouse.definitions import defs


def test_definitions_load() -> None:
    defs.validate_loadable(defs)
    assert defs.get_assets_def("hello_delta") is not None


def test_raw_zip_asset_has_one_partition_per_data_set() -> None:
    from dagster import AssetKey

    from sec_lakehouse.sources.form13f.datasets import FORM13F_DATASETS

    raw = defs.get_assets_def(AssetKey(["raw", "form13f_dataset_zip"]))
    assert raw.partitions_def is not None
    assert raw.partitions_def.get_partition_keys() == [d.key for d in FORM13F_DATASETS]
