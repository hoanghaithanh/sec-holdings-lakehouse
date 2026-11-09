"""Unpack a 13F data-set zip into line-numbered TSV files for Spark (ADR-003).

Runs on the driver and streams line by line, so memory stays flat whatever the zip size. Each
data line is written as `<line number>\\t<original line>`: the line number lets every Bronze row
point back to its place in the file, and nothing else about the line is changed. The header is
checked against the declared columns before anything is written to Bronze.
"""

from __future__ import annotations

import logging
import zipfile
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from sec_lakehouse.sources.form13f.schemas import TABLES, TableSpec

log = logging.getLogger(__name__)


class Form13fFormatError(ValueError):
    """The zip does not have the tables or columns the schemas declare."""


@dataclass(frozen=True)
class UnpackedTable:
    spec: TableSpec
    member: str  # file name inside the zip
    header: tuple[str, ...]  # column names as in the file, lower-cased
    rows: int  # data lines (header excluded)
    path: Path  # local line-numbered file


def find_members(names: list[str]) -> tuple[dict[str, str], list[str]]:
    """Map each table to its member in the zip. Returns (table name -> member, unknown members)."""
    by_stem: dict[str, list[str]] = {}
    for name in names:
        if name.endswith("/"):
            continue  # a folder entry
        by_stem.setdefault(PurePosixPath(name).stem.upper(), []).append(name)
    found: dict[str, str] = {}
    missing: list[str] = []
    for spec in TABLES:
        matches = [name for m in spec.members for name in by_stem.get(m, [])]
        if len(matches) != 1:
            missing.append(spec.members[0] if not matches else f"{spec.name} ({matches})")
            continue
        found[spec.name] = matches[0]
    if missing:
        raise Form13fFormatError(f"zip is missing or has duplicate tables: {', '.join(missing)}")
    used = set(found.values())
    unknown = sorted(n for n in names if n not in used and not n.endswith("/"))
    return found, unknown


def check_header(spec: TableSpec, header: tuple[str, ...]) -> None:
    """Fail unless the header has exactly the declared columns (optional ones may be absent)."""
    seen = set(header)
    if len(seen) != len(header):
        raise Form13fFormatError(f"{spec.name}: duplicate columns in header {header}")
    declared = set(spec.columns)
    missing = sorted(declared - seen - spec.optional)
    extra = sorted(seen - declared)
    if missing or extra:
        raise Form13fFormatError(
            f"{spec.name}: header does not match the declared schema "
            f"(missing: {missing or 'none'}, unexpected: {extra or 'none'}). "
            "Update sources/form13f/schemas.py on purpose if SEC changed the format."
        )


def _lines(archive: zipfile.ZipFile, member: str) -> Iterator[str]:
    with archive.open(member) as handle:
        for number, raw in enumerate(handle, start=1):
            try:
                text = raw.decode("utf-8-sig" if number == 1 else "utf-8")
            except UnicodeDecodeError as exc:
                raise Form13fFormatError(f"{member} line {number} is not UTF-8: {exc}") from exc
            yield text.removesuffix("\n").removesuffix("\r")


def unpack(zip_path: Path, out_dir: Path) -> list[UnpackedTable]:
    """Check and unpack all 7 tables. Nothing is written if any header is wrong."""
    out_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path) as archive:
        members, unknown = find_members(archive.namelist())
        if unknown:
            log.info("Not loaded (unknown members): %s", unknown)

        headers: dict[str, tuple[str, ...]] = {}
        for spec in TABLES:
            first = next(_lines(archive, members[spec.name]), "")
            headers[spec.name] = tuple(col.strip().lower() for col in first.split("\t"))
            check_header(spec, headers[spec.name])

        tables: list[UnpackedTable] = []
        for spec in TABLES:
            path = out_dir / f"{spec.name}.tsv"
            rows = 0
            with path.open("w", encoding="utf-8", newline="\n") as out:
                lines = _lines(archive, members[spec.name])
                next(lines)  # header, already checked
                for number, line in enumerate(lines, start=2):
                    out.write(f"{number}\t{line}\n")
                    rows += 1
            tables.append(UnpackedTable(spec, members[spec.name], headers[spec.name], rows, path))
    return tables
