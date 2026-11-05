from sec_lakehouse.definitions import defs


def test_definitions_load() -> None:
    defs.validate_loadable(defs)
    assert defs.get_assets_def("hello_delta") is not None
