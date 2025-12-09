import smartsheet
from datetime import date
from typing import Dict, Any, List

from config import SMARTSHEET_ACCESS_TOKEN, SMARTSHEET_CONFIG

# Initialize Smartsheet client
client = smartsheet.Smartsheet(SMARTSHEET_ACCESS_TOKEN)
client.errors_as_exceptions(True)


def get_sheet(sheet_key: str):
    """
    Generic helper: given a key from SMARTSHEET_CONFIG, fetch and return the
    Smartsheet sheet object.
    """
    sheet_id = SMARTSHEET_CONFIG[sheet_key]
    sheet = client.Sheets.get_sheet(sheet_id)
    return sheet


def sheet_to_records(sheet) -> List[Dict[str, Any]]:
    """
    Convert a Smartsheet sheet into a list of dicts where each dict is:
        {column_title: cell_value, ...}
    """
    column_map = {col.id: col.title for col in sheet.columns}
    records: List[Dict[str, Any]] = []

    for row in sheet.rows:
        record: Dict[str, Any] = {}
        for cell in row.cells:
            col_name = column_map[cell.column_id]
            record[col_name] = cell.value
        records.append(record)

    return records


# -------------------------------------------------------------------
# Referral sheet: General Shelter Referral Forms
# -------------------------------------------------------------------

def get_shelter_sheet():
    """Raw sheet object for the referral form."""
    return get_sheet("General_Shelter_Referral_Forms")


def get_shelter_referrals() -> List[Dict[str, Any]]:
    """Referral rows as list of dicts."""
    sheet = get_shelter_sheet()
    return sheet_to_records(sheet)


# -------------------------------------------------------------------
# Shelter Database
# -------------------------------------------------------------------

def get_shelter_database_sheet():
    """Raw sheet object for the Shelter Database."""
    return get_sheet("Shelter_Database")


def get_shelter_database() -> List[Dict[str, Any]]:
    """Shelter database rows as list of dicts."""
    sheet = get_shelter_database_sheet()
    return sheet_to_records(sheet)


# -------------------------------------------------------------------
# Shelter Bed Availability
# -------------------------------------------------------------------

def get_bed_availability_sheet():
    """Raw sheet object for the Shelter Bed Availability."""
    return get_sheet("Shelter_Bed_Availability")


def get_bed_availability_raw() -> List[Dict[str, Any]]:
    """Bed availability rows exactly as in Smartsheet (no normalization)."""
    sheet = get_bed_availability_sheet()
    return sheet_to_records(sheet)


def normalize_bed_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize a bed availability row into a standard structure:

    {
      "shelter_uid": "SHELTER-001",
      "beds_available": 10,
      "date_str": "2025-11-19",
      "date": datetime.date(2025, 11, 19)
    }

    Assumes ShelterID in Smartsheet is like SHELTER-001 (you just fixed it).
    If you ever get SHELTER_001 style again, you can .replace("_", "-").
    """
    raw_id = row.get("ShelterID")
    if raw_id is None:
        shelter_uid = None
    else:
        # In case future data uses underscores, normalize anyway:
        shelter_uid = str(raw_id).replace("_", "-")

    beds = row.get("BedsAvailable")
    try:
        beds_available = int(beds) if beds is not None else 0
    except (TypeError, ValueError):
        beds_available = 0

    date_str = row.get("Date")
    parsed_date = None
    if date_str:
        try:
            parsed_date = date.fromisoformat(date_str)
        except ValueError:
            parsed_date = None

    return {
        "shelter_uid": shelter_uid,
        "beds_available": beds_available,
        "date_str": date_str,
        "date": parsed_date,
    }


def get_bed_availability() -> List[Dict[str, Any]]:
    """
    Returns a list of normalized bed availability records.
    """
    raw_rows = get_bed_availability_raw()
    return [normalize_bed_row(r) for r in raw_rows]


def get_latest_beds_by_shelter() -> Dict[str, Dict[str, Any]]:
    """
    Returns a dict keyed by shelter_uid with the most recent bed count:

    {
      "SHELTER-001": {"date": date(...), "beds_available": 27},
      "SHELTER-002": {"date": date(...), "beds_available": 10},
      ...
    }
    """
    latest: Dict[str, Dict[str, Any]] = {}

    for row in get_bed_availability():
        uid = row["shelter_uid"]
        d = row["date"]
        if uid is None or d is None:
            continue

        if uid not in latest or d > latest[uid]["date"]:
            latest[uid] = {
                "date": d,
                "beds_available": row["beds_available"],
            }

    return latest
