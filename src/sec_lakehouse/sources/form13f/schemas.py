"""Declared columns of the 7 tables in each SEC Form 13F data-set zip (spec, ADR-002).

Column names and order follow the SEC readme (https://www.sec.gov/files/form_13f_readme.pdf,
read on 2026-11-09), lower-cased. Bronze stores every column as a string, exactly as in the file.

Not yet checked against a real zip (sec.gov is blocked from the build sandbox), so the first
real load confirms three assumptions, each of which fails loudly rather than silently if wrong:
- Each table is a member named after it (`SUBMISSION.tsv`); the extension is not checked.
  The readme calls the signature table both SIGNATURE and SIGNATUREBLOCK, so both are accepted.
- Header names match the readme. Headers are matched by name, not position.
- `figi` may be missing from older data sets (FIGI was added to the 13F XML later). Only
  columns listed in `optional` may be missing; they load as null. Any other missing or extra
  column fails the partition.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class TableSpec:
    name: str  # Bronze table suffix: bronze/form13f_<name>
    members: tuple[str, ...]  # accepted member file stems in the zip, upper case
    columns: tuple[str, ...]  # declared columns, lower case, in readme order
    keys: tuple[str, ...]  # unique within one data set (checked in a later slice)
    optional: frozenset[str] = field(default_factory=frozenset)


_MANAGER_IDS = ("cik", "form13ffilenumber", "crdnumber", "secfilenumber", "name")

TABLES: tuple[TableSpec, ...] = (
    TableSpec(
        "submission",
        ("SUBMISSION",),
        ("accession_number", "filing_date", "submissiontype", "cik", "periodofreport"),
        ("accession_number",),
    ),
    TableSpec(
        "coverpage",
        ("COVERPAGE",),
        (
            "accession_number",
            "reportcalendarorquarter",
            "isamendment",
            "amendmentno",
            "amendmenttype",
            "confdeniedexpired",
            "datedeniedexpired",
            "datereported",
            "reasonfornonconfidentiality",
            "filingmanager_name",
            "filingmanager_street1",
            "filingmanager_street2",
            "filingmanager_city",
            "filingmanager_stateorcountry",
            "filingmanager_zipcode",
            "reporttype",
            "form13ffilenumber",
            "crdnumber",
            "secfilenumber",
            "provideinfoforinstruction5",
            "additionalinformation",
        ),
        ("accession_number",),
    ),
    TableSpec(
        "othermanager",
        ("OTHERMANAGER",),
        ("accession_number", "othermanager_sk", *_MANAGER_IDS),
        ("accession_number", "othermanager_sk"),
    ),
    TableSpec(
        "signature",
        ("SIGNATURE", "SIGNATUREBLOCK"),
        (
            "accession_number",
            "name",
            "title",
            "phone",
            "signature",
            "city",
            "stateorcountry",
            "signaturedate",
        ),
        ("accession_number",),
    ),
    TableSpec(
        "summarypage",
        ("SUMMARYPAGE",),
        (
            "accession_number",
            "otherincludedmanagerscount",
            "tableentrytotal",
            "tablevaluetotal",
            "isconfidentialomitted",
        ),
        ("accession_number",),
    ),
    TableSpec(
        "othermanager2",
        ("OTHERMANAGER2",),
        ("accession_number", "sequencenumber", *_MANAGER_IDS),
        ("accession_number", "sequencenumber"),  # readme marks only accession_number: unverified
    ),
    TableSpec(
        "infotable",
        ("INFOTABLE",),
        (
            "accession_number",
            "infotable_sk",
            "nameofissuer",
            "titleofclass",
            "cusip",
            "figi",
            "value",
            "sshprnamt",
            "sshprnamttype",
            "putcall",
            "investmentdiscretion",
            "othermanager",
            "voting_auth_sole",
            "voting_auth_shared",
            "voting_auth_none",
        ),
        ("accession_number", "infotable_sk"),
        optional=frozenset({"figi"}),
    ),
)

METADATA_COLUMNS = ("_dataset", "_source_file", "_source_line", "_ingested_at", "_run_id")


def bronze_table_name(spec: TableSpec) -> str:
    return f"form13f_{spec.name}"
