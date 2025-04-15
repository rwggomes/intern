
# ruff: noqa
import os
from datetime import datetime
from django.db import transaction, IntegrityError
from moxie.utils.medspa_data_import.sheet import Sheet
from moxie.medspas.models import Medspa, Client

# Required fields for validation
REQUIRED_MEDSPA_FIELDS = ["name", "location"]
REQUIRED_CLIENT_FIELDS = [
    "first_name", "last_name", "email", "phone", "birthdate",
    "address", "city", "state", "zip_code", "note", "medspa_name"
]

# Function to check if the row has valid data
def validate_row_fields(row_data, required_fields, context="client"):
    field_errors = {}

    for field in required_fields:
        value = row_data.get(field)

        if value is None or (isinstance(value, str) and not value.strip()):
            field_errors[field] = f"This field is required for {context}"
            continue

        # Type or format checks
        if field in ["first_name", "last_name", "address", "city", "state", "note", "medspa_name"]:
            if not isinstance(value, str):
                field_errors[field] = "Must be a string"

        elif field == "email":
            if not isinstance(value, str) or "@" not in value or "." not in value:
                field_errors[field] = "Invalid email format"

        elif field == "phone":
            digits = ''.join(c for c in str(value) if c.isdigit())
            if len(digits) < 7:
                field_errors[field] = "Phone number must have at least 7 digits"

        elif field == "zip_code":
            zip_str = str(value).strip()
            if not zip_str.isdigit() or len(zip_str) not in [5, 9]:
                field_errors[field] = "ZIP code must be a 5- or 9-digit number"

        elif field == "birthdate":
            if isinstance(value, str):
                try:
                    datetime.strptime(value.strip(), "%m/%d/%Y")
                except ValueError:
                    field_errors[field] = "Birthdate must be in MM/DD/YYYY format"
            else:
                field_errors[field] = "Birthdate must be a string in MM/DD/YYYY format"

    return field_errors

# Main function that performs the import
def run_medspa_client_import():
    spreadsheet = Sheet(os.getenv("1sz9O9fbbAUkQxUlDjz6DSphK502lM3O65MYO5YRcXXU"))

    error_log = {
        "medspas": [],
        "clients": []
    }

    import_stats = {
        "medspas_created": 0,
        "clients_created": 0,
        "medspas_failed": 0,
        "clients_failed": 0
    }

    medspa_name_to_id = {}

    # Load rows from each tab in the sheet
    medspa_rows = spreadsheet.read_sheet("MedSpas")
    client_rows = spreadsheet.read_sheet("Clients")

    with transaction.atomic():
        # Step 1: Create MedSpas
        for index, row in enumerate(medspa_rows, start=2):
            issues = validate_row_fields(row, REQUIRED_MEDSPA_FIELDS, context="medspa")
            if issues:
                error_log["medspas"].append({"row": index, "errors": issues})
                import_stats["medspas_failed"] += 1
                continue

            try:
                new_medspa = Medspa.objects.create(
                    name=row["name"].strip(),
                    location=row["location"].strip()
                )
                medspa_name_to_id[new_medspa.name.strip()] = new_medspa.id
                import_stats["medspas_created"] += 1
            except IntegrityError as err:
                error_log["medspas"].append({"row": index, "errors": str(err)})
                import_stats["medspas_failed"] += 1

        # Step 2: Create Clients
        for index, row in enumerate(client_rows, start=2):
            issues = validate_row_fields(row, REQUIRED_CLIENT_FIELDS, context="client")
            if issues:
                error_log["clients"].append({"row": index, "errors": issues})
                import_stats["clients_failed"] += 1
                continue

            linked_medspa_name = row["medspa_name"].strip()
            linked_medspa_id = medspa_name_to_id.get(linked_medspa_name)

            if not linked_medspa_id:
                existing_medspa = Medspa.objects.filter(name=linked_medspa_name).first()
                if existing_medspa:
                    linked_medspa_id = existing_medspa.id
                else:
                    error_log["clients"].append({
                        "row": index,
                        "errors": {"medspa_name": f"MedSpa '{linked_medspa_name}' not found in import or database"}
                    })
                    import_stats["clients_failed"] += 1
                    continue

            try:
                Client.objects.create(
                    first_name=row["first_name"].strip(),
                    last_name=row["last_name"].strip(),
                    email=row["email"].strip(),
                    phone=row["phone"].strip(),
                    birthdate=datetime.strptime(row["birthdate"].strip(), "%m/%d/%Y").date(),
                    address=row["address"].strip(),
                    city=row["city"].strip(),
                    state=row["state"].strip(),
                    zip_code=row["zip_code"].strip(),
                    note=row["note"].strip(),
                    medspa_id=linked_medspa_id
                )
                import_stats["clients_created"] += 1
            except IntegrityError as err:
                error_log["clients"].append({"row": index, "errors": str(err)})
                import_stats["clients_failed"] += 1

    return import_stats, error_log
