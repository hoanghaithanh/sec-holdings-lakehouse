from __future__ import annotations

from pathlib import Path

import pytest
from form13f_zips import build_zip, table_lines

from sec_lakehouse.sources.form13f.schemas import TABLES
from sec_lakehouse.sources.form13f.unpack import Form13fFormatError, find_members, unpack

SPECS = {spec.name: spec for spec in TABLES}


def write_zip(tmp_path: Path, data: bytes) -> Path:
    path = tmp_path / "test_form13f.zip"
    path.write_bytes(data)
    return path


def test_unpacks_all_tables_with_line_numbers(tmp_path: Path) -> None:
    tables = unpack(write_zip(tmp_path, build_zip(rows=3)), tmp_path / "out")

    assert [t.spec.name for t in tables] == [s.name for s in TABLES]
    infotable = next(t for t in tables if t.spec.name == "infotable")
    assert infotable.rows == 3
    assert infotable.header == SPECS["infotable"].columns
    lines = infotable.path.read_text(encoding="utf-8").splitlines()
    assert lines[0].split("\t")[0] == "2"  # header is line 1
    assert lines[0].split("\t", 1)[1] == table_lines(SPECS["infotable"], 3)[1]


def test_crlf_and_bom_are_not_part_of_the_data(tmp_path: Path) -> None:
    lines = table_lines(SPECS["submission"], 1)
    lines[0] = "﻿" + lines[0]
    data = build_zip(rows=1, overrides={"submission": lines}, crlf=True)
    tables = unpack(write_zip(tmp_path, data), tmp_path / "out")
    submission = tables[0]
    assert submission.header == SPECS["submission"].columns
    content = submission.path.read_bytes()
    assert b"\r" not in content
    assert content.decode().rstrip("\n").endswith("periodofreport 0")


def test_values_are_kept_exactly(tmp_path: Path) -> None:
    lines = table_lines(SPECS["submission"], 1)
    lines[1] = '0000950123-13-000001\t  15-MAY-2013\t"13F-HR"\t\t'  # spaces, quotes, empty fields
    data = build_zip(rows=1, overrides={"submission": lines})
    tables = unpack(write_zip(tmp_path, data), tmp_path / "out")
    staged = tables[0].path.read_text(encoding="utf-8")
    assert staged == '2\t0000950123-13-000001\t  15-MAY-2013\t"13F-HR"\t\t\n'


def test_header_matched_by_name_and_case(tmp_path: Path) -> None:
    spec = SPECS["submission"]
    reordered = list(reversed(spec.columns))
    lines = ["\t".join(c.lower() for c in reordered), "\t".join(f"v{i}" for i in range(5))]
    tables = unpack(write_zip(tmp_path, build_zip(overrides={"submission": lines})), tmp_path / "o")
    assert tables[0].header == tuple(reordered)


@pytest.mark.parametrize(
    ("change", "message"),
    [
        (lambda cols: cols[:-1], "missing: \\['periodofreport'\\]"),
        (lambda cols: [*cols, "NEWCOLUMN"], "unexpected: \\['newcolumn'\\]"),
        (lambda cols: [*cols, cols[0]], "duplicate"),
    ],
)
def test_header_mismatch_fails_before_writing(tmp_path: Path, change, message: str) -> None:  # type: ignore[no-untyped-def]
    spec = SPECS["submission"]
    header = change([c.upper() for c in spec.columns])
    lines = ["\t".join(header), "\t".join("x" for _ in header)]
    out = tmp_path / "out"
    with pytest.raises(Form13fFormatError, match=message):
        unpack(write_zip(tmp_path, build_zip(overrides={"submission": lines})), out)
    assert not any(out.glob("*.tsv"))


def test_optional_figi_may_be_missing(tmp_path: Path) -> None:
    spec = SPECS["infotable"]
    columns = [c for c in spec.columns if c != "figi"]
    lines = ["\t".join(c.upper() for c in columns), "\t".join("x" for _ in columns)]
    tables = unpack(write_zip(tmp_path, build_zip(overrides={"infotable": lines})), tmp_path / "o")
    assert "figi" not in tables[-1].header


def test_member_names_and_signatureblock_alias() -> None:
    names = [f"{s.members[0]}.tsv" for s in TABLES if s.name != "signature"]
    names += ["SIGNATUREBLOCK.txt", "FORM13F_metadata.json", "docs/"]
    members, unknown = find_members(names)
    assert members["signature"] == "SIGNATUREBLOCK.txt"
    assert unknown == ["FORM13F_metadata.json"]


def test_duplicate_members_fail() -> None:
    names = [f"{s.members[0]}.tsv" for s in TABLES] + ["old/INFOTABLE.txt"]
    with pytest.raises(Form13fFormatError, match="duplicate"):
        find_members(names)


def test_missing_table_fails() -> None:
    names = [f"{s.members[0]}.tsv" for s in TABLES if s.name != "infotable"]
    with pytest.raises(Form13fFormatError, match="INFOTABLE"):
        find_members(names)


def test_non_utf8_fails_with_line_number(tmp_path: Path) -> None:
    import io
    import zipfile

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for spec in TABLES:
            text = "\n".join(table_lines(spec, 1)) + "\n"
            data = text.encode() if spec.name != "coverpage" else text.encode() + b"\xff\xfe\n"
            archive.writestr(f"{spec.members[0]}.tsv", data)
    with pytest.raises(Form13fFormatError, match="line 3 is not UTF-8"):
        unpack(write_zip(tmp_path, buffer.getvalue()), tmp_path / "out")
