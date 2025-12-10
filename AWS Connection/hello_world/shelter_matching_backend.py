"""
shelter_matching_backend.py

Backend-only script to:
1. Pull referral rows and shelter database rows (from Smartsheet or other source).
2. Pull latest beds by shelter (one record per shelter_uid).
3. Apply STRICT / exact matching filters using all overlapping fields.
4. Write an Excel file of only valid matches (no scores, no ranking).
"""

from __future__ import annotations

import sys
import re
from datetime import date
from typing import Dict, Any, List, Tuple

import pandas as pd

from smartsheet_client import (
    get_shelter_referrals,
    get_shelter_database,
    get_latest_beds_by_shelter,
)

# ---------- Helper conversions ----------


def _to_bool(val: Any) -> bool:
    """Convert Smartsheet-style TRUE/FALSE/Yes/No etc. into a real bool."""
    if isinstance(val, bool):
        return val
    if val is None:
        return False
    s = str(val).strip().lower()
    return s in {"true", "yes", "y", "1"}


def _to_int_or_none(val):
    """Convert numeric-ish values to int, including floats like 5.0."""
    # Treat None / NaN as missing
    if val is None:
        return None

    if isinstance(val, float):
        if pd.isna(val):
            return None
        # e.g., 5.0 -> 5
        return int(val)

    if isinstance(val, int):
        return val

    s = str(val).strip()
    if not s:
        return None

    # First try direct int cast
    try:
        return int(s)
    except ValueError:
        # Handle things like "5.0"
        try:
            return int(float(s))
        except ValueError:
            return None


def _split_tags(val: Any) -> List[str]:
    """
    Split comma-separated tag fields into a normalized lowercase list.

    Handles:
    - None / NaN => []
    - Strings like 'A,B,C'
    - Ignores 'nan' / 'none' as real tags
    """
    # Treat None and pandas NaN as empty
    if val is None:
        return []
    if isinstance(val, float) and pd.isna(val):
        return []

    s = str(val).strip().lower()
    if not s:
        return []

    # Often Smartsheet / pandas NaNs become 'nan'
    if s in {"nan", "none"}:
        return []

    return [p.strip() for p in s.split(",") if p.strip()]


# ---------- Normalization: Referrals ----------


def _extract_spa_from_location(loc: Any) -> int | None:
    """
    From strings like 'SPA 4 - Skid Row Only' or 'SPA 5 - West LA',
    extract the numeric SPA id as int.
    """
    if loc is None:
        return None
    s = str(loc)
    m = re.search(r"spa\s*(\d+)", s, flags=re.IGNORECASE)
    if m:
        try:
            return int(m.group(1))
        except ValueError:
            return None
    return None


def normalize_referrals_df(raw_df: pd.DataFrame) -> pd.DataFrame:
    """
    Map raw Smartsheet referral columns -> normalized fields used by the matcher.
    """
    df = pd.DataFrame()

    # Basic identity
    df["ref_row_id"] = raw_df.get("uid")
    df["ref_first_name"] = raw_df.get("first_name")
    df["ref_last_name"] = raw_df.get("last_name")

    # Demographics
    df["ref_age"] = raw_df.get("age")
    df["ref_gender_identity"] = raw_df.get("gender_identity")
    df["ref_gender_bed_pref"] = raw_df.get("gender_bed_preference")

    # Location / SPA
    loc_col = raw_df.get("current_stay")
    df["ref_current_location"] = loc_col
    if loc_col is not None:
        df["ref_spa"] = loc_col.apply(_extract_spa_from_location)
    else:
        df["ref_spa"] = None
    df["ref_preferred_spa"] = raw_df.get("preferred_spa")
    df["ref_preferred_city"] = raw_df.get("preferred_city_name")
    df["ref_exclude_spa"] = raw_df.get("exclude_spa")
    df["ref_exclude_city_name"] = raw_df.get("exclude_city_name")
    df["ref_skid_row"] = raw_df.get("skid_row")

    # Vehicle
    df["ref_vehicle"] = raw_df.get("vehicle")
    df["ref_vehicle_info"] = raw_df.get("vehicle_info")

    # Animals – collapse multiple Y/N fields into a single boolean
    animal_cols = [
        "animals",
        "service_animal",
        "emotional_supportemotional_support_animal",
        "pet",
    ]

    # health
    df["ref_health"] = raw_df.get("health_concerns")
    df["ref_accessibility"] = raw_df.get("accessibility")
    df["ref_room_type"] = raw_df.get("congregate_environment")
    df["ref_top_bunk"] = raw_df.get("top_bunk")


    def _has_any_animal(row: pd.Series) -> bool:
        for col in animal_cols:
            if col in row.index:
                if _to_bool(row[col]):
                    return True
        return False

    df["ref_has_any_animal"] = raw_df.apply(_has_any_animal, axis=1)

    # Presenting issues
    df["ref_presenting_issues"] = raw_df.get("special_situations")

    return df


# ---------- Normalization: Shelter Database ----------


def normalize_shelters_df(raw_df: pd.DataFrame) -> pd.DataFrame:
    """
    Map raw Smartsheet shelter columns -> normalized fields used by the matcher.
    """
    df = pd.DataFrame()

    df["shelter_uid"] = raw_df.get("shelter_id")
    df["shelter_name"] = raw_df.get("shelter_name")

    # SPA + cities
    spa_col = raw_df.get("spa")
    if spa_col is not None:
        df["shelter_spa"] = spa_col.apply(_to_int_or_none)
    else:
        df["shelter_spa"] = None
    df["shelter_cities"] = raw_df.get("cities")

    # Pets / demographics / programs / entry requirements
    df["shelter_age"] = raw_df.get("age_group") 
    df["shelter_gender"] = raw_df.get("gender")
    #df["shelter_sexual_orientation"] = raw_df.get("sexual_orientation")
    #df["shelter_programs"] = raw_df.get("shelter_programs")
    df["shelter_entry_requirements"] = raw_df.get("entry_requirements")
    df["shelter_special_situation_restrictions"] = raw_df.get("special_situation_restrictions")
    df["shelter_accessibility"] = raw_df.get("accessibility")
    df["shelter_health_services"] = raw_df.get("health_services")
    df["shelter_room_style"] = raw_df.get("room_style")
    df["shelter_storage"] = raw_df.get("storage")
    df["shelter_pets"] = raw_df.get("pets")
    df["shelter_meals"] = raw_df.get("meals")
    df["shelter_parking"] = raw_df.get("parking")
    df["shelter_vehicles"] = raw_df.get("vehicles")
    df["shelter_criminal_history"] = raw_df.get("criminal_history")

    # Contact info
    df["shelter_email"] = raw_df.get("email")
    df["shelter_phone"] = raw_df.get("phone")

    return df


# ---------- Core match logic ----------


def is_exact_match(
    ref_row: Dict[str, Any],
    shelter_row: Dict[str, Any],
    beds_by_shelter: Dict[str, Dict[str, Any]],
) -> bool:
    """
    Return True only if this shelter is a valid, *exact* match for the referral,
    based on all overlapping filters we can safely enforce:

    1. Beds available > 0
    2. SPA match (shelter must have a SPA and it must equal the referral SPA)
    3. Pets compatibility
    4. Vehicle vs safe parking / vehicle shelters
    5. Gender (bed preference or identity) vs shelter demographics
    6. Age vs seniors / TAY / youth-only programs
    """

    # ---------- 1. Beds available ----------
    uid = shelter_row.get("shelter_uid")
    if not uid:
        return False

    bed_info = beds_by_shelter.get(uid, {})
    beds_available = bed_info.get("beds_available", 0)
    if beds_available is None:
        beds_available = 0

    if beds_available <= 0:
        return False

    # ---------- 2. SPA match (strict) ----------

    ref_exclude_spa = _split_tags(ref_row.get("ref_exclude_spa"))
    shelter_spa = shelter_row.get("shelter_spa")

    # For strict matching: if we know the referral SPA, the shelter MUST
    # have a SPA and it MUST match.
    if ref_exclude_spa != []:
        if shelter_spa is None or shelter_spa in ref_exclude_spa:
            return False

    # ---------- 3. Pets compatibility ----------
    has_animal = _to_bool(ref_row.get("ref_has_any_animal"))
    shelter_pets_tags = _split_tags(shelter_row.get("shelter_pets"))

    if has_animal:
        # If they have an animal, the shelter must allow *some* type of pet
        # and not have a "no_pets_allowed" flag.
        if not shelter_pets_tags:
            return False
        if "no_pets_allowed" in shelter_pets_tags:
            return False

    # ---------- 4. Vehicle vs safe parking / vehicle shelters ----------
    has_vehicle = _to_bool(ref_row.get("ref_vehicle"))
    shelter_programs_tags = _split_tags(shelter_row.get("shelter_programs"))
    shelter_entry_reqs_tags = _split_tags(shelter_row.get("shelter_entry_requirements"))

    if has_vehicle:
        # If client DOES have a vehicle, require a vehicle-friendly program.
        supports_vehicle = (
            any("safe_park" in p for p in shelter_programs_tags)
            or any("safe parking" in p for p in shelter_programs_tags)
            or "vehicle_registration" in shelter_entry_reqs_tags
        )
        if not supports_vehicle:
            return False
    else:
        # Client does NOT have a vehicle: do NOT match safe-parking-only shelters.
        is_safe_parking_shelter = any(
            ("safe_park" in p) or ("safe parking" in p) for p in shelter_programs_tags
        )
        if is_safe_parking_shelter:
            return False

    # ---------- 5. Gender vs shelter demographics ----------
    gender_bed_pref_raw = (ref_row.get("ref_gender_bed_pref") or "").strip().lower()
    gender_identity_raw = (ref_row.get("ref_gender_identity") or "").strip().lower()

    # If bed preference is blank or "no preference", fall back to gender identity.
    gender_source = gender_bed_pref_raw
    if (not gender_source) or ("no preference" in gender_source):
        gender_source = gender_identity_raw

    shelter_demo_tags = _split_tags(shelter_row.get("shelter_demographics"))

    shelter_age = _split_tags(shelter_row.get("age_group"))
    shelter_gender = _split_tags(shelter_row.get("shelter_gender"))

    # Treat empty demographics as "all" (inclusive) so we don't over-exclude.
    if not shelter_age:
        shelter_age = ["all"]
    if not shelter_gender:
        shelter_gender = ["all"]

    # Helper: does this shelter serve families / parents at all?
    def _serves_families(tags: list[str]) -> bool:
        family_like = {"families", "single_moms", "single_parents"}
        return any(t in family_like for t in tags)

    # If demographics are clearly TAY or seniors focused, allow any gender.
    # Age logic (step 6) will further restrict.
    is_tay_focused = "tay_teen" in shelter_age
    is_senior_focused = "seniors" in shelter_age

    if gender_source and not (is_tay_focused or is_senior_focused):
        # Basic male/female inference from text.
        is_male_pref = any(kw in gender_source for kw in ["male", "man", "boy"])
        is_female_pref = any(kw in gender_source for kw in ["female", "woman", "girl"])

        if is_male_pref:
            # Require 'single_men' OR 'all' OR families
            if (
                "single_men" not in shelter_gender
                and "all" not in shelter_gender
                and not _serves_families(shelter_gender)
            ):
                return False

        if is_female_pref:
            # Require 'single_women' OR 'single_moms' OR 'families' OR 'all'
            if (
                "single_women" not in shelter_gender
                and "single_moms" not in shelter_gender
                and "all" not in shelter_gender
                and not _serves_families(shelter_gender)
            ):
                return False

    # ---------- 6. Age vs seniors / TAY / youth-only ----------
    age = _to_int_or_none(ref_row.get("ref_age"))

    if age is not None:
        # Seniors-only: only if the *only* tag is seniors
        is_seniors_only = shelter_age == ["seniors"]
        if is_seniors_only and age < 55:
            return False

        # TAY-only: only if the *only* tag is tay_teen
        is_tay_only = shelter_age == ["tay_teen"]
        if is_tay_only and age > 24:
            return False

    return True


# ---------- Debug version of matcher ----------


def debug_exact_match(
    ref_row: Dict[str, Any],
    shelter_row: Dict[str, Any],
    beds_by_shelter: Dict[str, Dict[str, Any]],
) -> Tuple[bool, str]:
    """
    Same logic as is_exact_match, but returns (ok, reason_for_failure_or_ok).
    Stops at first failure so we can see which filter is killing matches.
    """
    uid = shelter_row.get("shelter_uid") or "UNKNOWN_UID"

    # 1. Beds
    bed_info = beds_by_shelter.get(uid, {})
    beds_available = bed_info.get("beds_available", 0)
    if beds_available is None:
        beds_available = 0
    if beds_available <= 0:
        return False, "no beds available"

    # 2. SPA
    ref_exclude_spa = _to_int_or_none(ref_row.get("ref_exclude_spa"))
    shelter_spa = _to_int_or_none(shelter_row.get("shelter_spa"))
    if ref_exclude_spa is not None:
        if shelter_spa is None:
            return False, "shelter SPA missing"
        if shelter_spa == ref_exclude_spa:
            return False, f"SPA mismatch (ref exclude spa {ref_exclude_spa} vs shelter {shelter_spa})"

    # 3. Pets
    has_animal = _to_bool(ref_row.get("ref_has_any_animal"))
    shelter_pets_tags = _split_tags(shelter_row.get("shelter_pets"))
    if has_animal:
        if not shelter_pets_tags:
            return False, "client has animal but shelter has no pets field"
        if "no_pets_allowed" in shelter_pets_tags:
            return False, "client has animal but shelter has no_pets_allowed"

    # 4. Vehicle / safe parking
    has_vehicle = _to_bool(ref_row.get("ref_vehicle"))
    shelter_programs_tags = _split_tags(shelter_row.get("shelter_programs"))
    shelter_entry_reqs_tags = _split_tags(shelter_row.get("shelter_entry_requirements"))

    if has_vehicle:
        supports_vehicle = (
            any("safe_park" in p for p in shelter_programs_tags)
            or any("safe parking" in p for p in shelter_programs_tags)
            or "vehicle_registration" in shelter_entry_reqs_tags
        )
        if not supports_vehicle:
            return False, "client has vehicle but shelter not vehicle-friendly"
    else:
        is_safe_parking_shelter = any(
            ("safe_park" in p) or ("safe parking" in p) for p in shelter_programs_tags
        )
        if is_safe_parking_shelter:
            return False, "client no vehicle but shelter is safe-parking only"

    # 5. Gender / demographics
    gender_bed_pref_raw = (ref_row.get("ref_gender_bed_pref") or "").strip().lower()
    gender_identity_raw = (ref_row.get("ref_gender_identity") or "").strip().lower()
    gender_source = gender_bed_pref_raw
    if (not gender_source) or ("no preference" in gender_source):
        gender_source = gender_identity_raw

    if not shelter_age:
        shelter_age = ["all"]
    if not shelter_gender:
        shelter_gender = ["all"]

    shelter_demo_tags = _split_tags(shelter_row.get("shelter_demographics"))
    if not shelter_demo_tags:
        shelter_demo_tags = ["all"]

    def _serves_families(tags: list[str]) -> bool:
        family_like = {"families", "single_moms", "single_parents"}
        return any(t in family_like for t in tags)

    is_tay_focused = "tay_teen" in shelter_age
    is_senior_focused = "seniors" in shelter_age

    if gender_source and not (is_tay_focused or is_senior_focused):
        is_male_pref = any(kw in gender_source for kw in ["male", "man", "boy"])
        is_female_pref = any(kw in gender_source for kw in ["female", "woman", "girl"])

        if is_male_pref:
            if (
                "single_men" not in shelter_gender
                and "all" not in shelter_gender
                and not _serves_families(shelter_gender)
            ):
                return False, f"gender mismatch (male) vs {shelter_gender}"

        if is_female_pref:
            if (
                "single_women" not in shelter_gender
                and "single_moms" not in shelter_gender
                and "all" not in shelter_gender
                and not _serves_families(shelter_gender)
            ):
                return False, f"gender mismatch (female) vs {shelter_gender}"

    # 6. Age / TAY / seniors
    age = _to_int_or_none(ref_row.get("ref_age"))
    if age is not None:
        is_seniors_only = shelter_age == ["seniors"]
        if is_seniors_only and age < 55:
            return False, f"age {age} < 55 for seniors-only shelter"

        is_tay_only = shelter_age == ["tay_teen"]
        if is_tay_only and age > 24:
            return False, f"age {age} > 24 for TAY-only shelter"

    return True, "OK"


# Predefined column order for match output
MATCH_COLUMNS: List[str] = [
    # ----- Referral info -----
    "ref_row_id",
    "ref_first_name",
    "ref_last_name",
    "ref_age",
    "ref_gender_identity",
    "ref_gender_bed_pref",
    "ref_spa",
    "ref_vehicle",
    "ref_has_any_animal",
    "ref_current_location",
    "ref_presenting_issues",
    # ----- Shelter info -----
    "shelter_uid",
    "shelter_name",
    "shelter_spa",
    "shelter_cities",
    "shelter_pets",
    "shelter_demographics",
    "shelter_programs",
    "shelter_entry_requirements",
    "shelter_email",
    "shelter_phone",
    # ----- Beds info -----
    "beds_available",
    "beds_last_updated",
]


# ---------- Matching orchestration ----------


def build_matches(
    referrals_df: pd.DataFrame,
    shelters_df: pd.DataFrame,
    beds_by_shelter: Dict[str, Dict[str, Any]],
) -> pd.DataFrame:
    """
    Loop over all referral rows × all shelter rows and keep only exact matches.
    Output: DataFrame with one row per (referral, shelter) pair that passes filters.
    """
    referrals = referrals_df.to_dict(orient="records")
    shelters = shelters_df.to_dict(orient="records")

    match_rows: List[Dict[str, Any]] = []

    for ref in referrals:
        for shelter in shelters:
            if not is_exact_match(ref, shelter, beds_by_shelter):
                continue

            uid = shelter.get("shelter_uid")
            bed_info = beds_by_shelter.get(uid, {})
            beds_available = bed_info.get("beds_available", 0)
            beds_last_updated = bed_info.get("date")

            match_rows.append(
                {
                    # ----- Referral info -----
                    "ref_row_id": ref.get("ref_row_id"),
                    "ref_first_name": ref.get("ref_first_name"),
                    "ref_last_name": ref.get("ref_last_name"),
                    "ref_age": ref.get("ref_age"),
                    "ref_gender_identity": ref.get("ref_gender_identity"),
                    "ref_gender_bed_pref": ref.get("ref_gender_bed_pref"),
                    "ref_spa": ref.get("ref_spa"),
                    "ref_vehicle": ref.get("ref_vehicle"),
                    "ref_has_any_animal": ref.get("ref_has_any_animal"),
                    "ref_current_location": ref.get("ref_current_location"),
                    "ref_presenting_issues": ref.get("ref_presenting_issues"),
                    # ----- Shelter info -----
                    "shelter_uid": shelter.get("shelter_uid"),
                    "shelter_name": shelter.get("shelter_name"),
                    "shelter_spa": shelter.get("shelter_spa"),
                    "shelter_cities": shelter.get("shelter_cities"),
                    "shelter_pets": shelter.get("shelter_pets"),
                    "shelter_demographics": shelter.get("shelter_demographics"),
                    "shelter_programs": shelter.get("shelter_programs"),
                    "shelter_entry_requirements": shelter.get(
                        "shelter_entry_requirements"
                    ),
                    "shelter_email": shelter.get("shelter_email"),
                    "shelter_phone": shelter.get("shelter_phone"),
                    # ----- Beds info -----
                    "beds_available": beds_available,
                    "beds_last_updated": beds_last_updated,
                }
            )

    if not match_rows:
        # Return an empty DataFrame *with* the expected columns,
        # so the Excel file has proper headers.
        return pd.DataFrame(columns=MATCH_COLUMNS)

    matches_df = pd.DataFrame(match_rows, columns=MATCH_COLUMNS)

    # Sort for readability
    matches_df = matches_df.sort_values(
        ["ref_row_id", "shelter_spa", "shelter_name"],
        kind="mergesort",
    )

    return matches_df


# ---------- Debug helper when no matches ----------


def print_debug_no_matches(
    referrals_df: pd.DataFrame,
    shelters_df: pd.DataFrame,
    beds_by_shelter: Dict[str, Dict[str, Any]],
) -> None:
    """
    When there are zero matches, walk through each referral × shelter and show
    why each shelter failed its match.
    """
    referrals = referrals_df.to_dict(orient="records")
    shelters = shelters_df.to_dict(orient="records")

    print("\n================ DEBUG: No matches found ================\n")

    for ref in referrals:
        ref_id = ref.get("ref_row_id")
        ref_name = f"{ref.get('ref_first_name')} {ref.get('ref_last_name')}"
        ref_spa = ref.get("ref_spa")
        print(f"Referral {ref_id} ({ref_name}), SPA={ref_spa}, age={ref.get('ref_age')}, "
              f"gender_pref={ref.get('ref_gender_bed_pref')}, gender_id={ref.get('ref_gender_identity')}")
        print("-" * 80)

        for shelter in shelters:
            uid = shelter.get("shelter_uid")
            name = shelter.get("shelter_name")
            spa = shelter.get("shelter_spa")
            ok, reason = debug_exact_match(ref, shelter, beds_by_shelter)
            status = "MATCH" if ok else f"NO ({reason})"
            print(f"{ref_id} -> {uid} ({name}), shelter_SPA={spa}: {status}")

        print("\n")


# ---------- Main entry point ----------


def main() -> None:
    print("Loading referral data...")
    referral_records = get_shelter_referrals()
    raw_referrals_df = pd.DataFrame(referral_records)
    referrals_df = normalize_referrals_df(raw_referrals_df)

    print("Loading shelter database...")
    shelter_records = get_shelter_database()
    raw_shelters_df = pd.DataFrame(shelter_records)
    print("\n=== DEBUG: raw_shelters_df columns ===")
    print(list(raw_shelters_df.columns))

    # Show any columns whose name looks like it might be SPA-related
    spa_like_cols = [c for c in raw_shelters_df.columns if "spa" in str(c).lower()]
    print("\n=== DEBUG: candidate SPA columns ===")
    print(spa_like_cols)

    for c in spa_like_cols:
        print(f"\n--- Sample values for column '{c}' ---")
        print(raw_shelters_df[c].dropna().head(20).to_list())

    shelters_df = normalize_shelters_df(raw_shelters_df)

    print("Loading latest beds by shelter...")
    beds_by_shelter = get_latest_beds_by_shelter()

    print("Building exact matches...")
    matches_df = build_matches(referrals_df, shelters_df, beds_by_shelter)

    if matches_df.empty:
        print("No exact matches found based on current filters.")
        # DEBUG: show why each shelter failed for each referral
        print_debug_no_matches(referrals_df, shelters_df, beds_by_shelter)

        output_path = f"shelter_matches_exact_{date.today().isoformat()}_EMPTY.xlsx"
        matches_df.to_excel(output_path, index=False)
        print(f"Empty match file written to {output_path}")
        return

    output_path = f"shelter_matches_exact_{date.today().isoformat()}.xlsx"
    matches_df.to_excel(output_path, index=False)
    print(f"Wrote {len(matches_df)} match rows to {output_path}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Error running shelter_matching_backend: {exc}", file=sys.stderr)

        raise
