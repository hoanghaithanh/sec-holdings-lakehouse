from sec_lakehouse.definitions import defs


def test_definitions_load() -> None:
    defs.validate_loadable(defs)
    assert defs.get_assets_def("hello_delta") is not None


def test_raw_zip_asset_has_one_partition_per_data_set() -> None:
    from dagster import AssetKey

    raw = defs.get_assets_def(AssetKey(["raw", "form13f_dataset_zip"]))
    assert raw.partitions_def is not None
    assert len(raw.partitions_def.get_partition_keys()) == 54
