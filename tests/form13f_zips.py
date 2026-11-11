"""Build small 13F data-set zips for tests, with the readme's headers and made-up rows."""

from __future__ import annotations

import io
import zipfile

from sec_lakehouse.sources.form13f.schemas import TABLES, TableSpec


def table_lines(spec: TableSpec, rows: int, accession: str = "0000950123-13-000001") -> list[str]:
    header = "\t".join(c.upper() for c in spec.columns)
    lines = [header]
    for i in range(rows):
        values = []
        for column in spec.columns:
            if column == "accession_number":
                values.append(accession)
            elif column.endswith("_sk") or column == "sequencenumber":
                values.append(str(i + 1))
            else:
                values.append(f"{column} {i}")
        lines.append("\t".join(values))
    return lines


def build_zip(
    rows: int = 3,
    overrides: dict[str, list[str]] | None = None,
    crlf: bool = False,
    member_names: dict[str, str] | None = None,
) -> bytes:
    """One member per table. `overrides` replaces a table's lines; `crlf` uses Windows endings."""
    buffer = io.BytesIO()
    newline = "\r\n" if crlf else "\n"
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for spec in TABLES:
            lines = (overrides or {}).get(spec.name) or table_lines(spec, rows)
            name = (member_names or {}).get(spec.name, f"{spec.members[0]}.tsv")
            archive.writestr(name, newline.join(lines) + newline)
    return buffer.getvalue()
