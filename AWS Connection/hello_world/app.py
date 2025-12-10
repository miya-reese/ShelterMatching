import json
import base64
import io
from datetime import date
from send_email import send_email

import pandas as pd

from shelter_matching_backend import (
    normalize_referrals_df,
    normalize_shelters_df,
    build_matches,
)
from smartsheet_client import (
    get_shelter_referrals,
    get_shelter_database,
    get_latest_beds_by_shelter,
)


def lambda_handler(event, context):
    """
    Handles:
    1. Smartsheet webhook challenge (header: Smartsheet-Hook-Challenge)
    2. Standard POST to generate an Excel of strict matches
    """
    print("Event:", json.dumps(event))

    headers = {k.lower(): v for k, v in (event.get("headers") or {}).items()}

    # --- 1. Smartsheet webhook challenge handshake ---
    challenge = headers.get("smartsheet-hook-challenge")
    if challenge:
        print(f"Received handshake challenge: {challenge}")
        return {
            "statusCode": 200,
            "headers": {
                "Smartsheet-Hook-Response": challenge
            },
            "body": ""
        }

    # --- 2. Normal invocation → run shelter matching and return Excel ---
    try:
        print("Loading referrals...")
        referral_records = get_shelter_referrals()
        referrals_df = normalize_referrals_df(pd.DataFrame(referral_records))

        print("Loading shelters...")
        shelter_records = get_shelter_database()
        shelters_df = normalize_shelters_df(pd.DataFrame(shelter_records))

        print("Loading beds...")
        beds_by_shelter = get_latest_beds_by_shelter()

        print("Building matches...")
        matches_df = build_matches(referrals_df, shelters_df, beds_by_shelter)

        # Create Excel in memory
        buffer = io.BytesIO()
        today_str = date.today().isoformat()
        file_name = f"shelter_matches_exact_{today_str}.xlsx"

        with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
            matches_df.to_excel(writer, index=False, sheet_name="matches")

        buffer.seek(0)
        excel_bytes = buffer.read()
        encoded = base64.b64encode(excel_bytes).decode("utf-8")
        send_email(excel_bytes, file_name)

        print(f"Returning Excel file: {file_name}, rows={len(matches_df)}")

        return {
            "statusCode": 200,
            "isBase64Encoded": True,
            "headers": {
                "Content-Type": (
                    "application/vnd.openxmlformats-officedocument."
                    "spreadsheetml.sheet"
                ),
                "Content-Disposition": f'attachment; filename="{file_name}"'
            },
            "body": encoded
        }

    except Exception as exc:
        print(f"Error in lambda_handler: {exc}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(exc)})
        }