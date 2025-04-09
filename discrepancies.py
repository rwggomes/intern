import pandas as pd

def read_file(path):
    return pd.read_csv(path, dtype=str).fillna("")

def clean_phone(phone):
    if phone and phone != "MISSING":
        return ''.join(c for c in phone if c.isdigit())
    return ""

def clean_name(name):
    return ' '.join(name.lower().strip().split())

def format_date(date):
    try:
        return pd.to_datetime(date).strftime("%m/%d/%Y")
    except:
        return date

def check_differences(row):
    issues = []
    if row["Name1"] != row["Name2"]:
        issues.append("Different name")
    if row["Phone1"] != row["Phone2"]:
        issues.append("Different phone")
    if row["Birth Date1"] != row["Birth Date2"]:
        issues.append("Different birthdate")
    return ", ".join(issues) if issues else "No differences"

def process(file1, file2, output_file):
    df1 = read_file(file1)
    df2 = read_file(file2)

    df1.rename(columns={
        "Client ID": "ID",
        "First Name": "First Name",
        "Last Name": "Last Name",
        "Email Address": "Email",
        "Phone Number": "Phone",
        "Date of Birth": "Birth Date"
    }, inplace=True)

    df2.rename(columns={
        "customer_id": "ID",
        "name": "Full Name",
        "contact_email": "Email",
        "mobile": "Phone",
        "birth_date": "Birth Date",
        "address": "Address",
        "city": "City",
        "state": "State",
        "postal_code": "ZIP"
    }, inplace=True)

    df1["Name"] = (df1["First Name"].fillna("") + " " + df1["Last Name"].fillna("")).apply(clean_name)
    df2["Name"] = df2["Full Name"].fillna("").apply(clean_name)

    df1["Phone"] = df1["Phone"].apply(clean_phone)
    df2["Phone"] = df2["Phone"].apply(clean_phone)
    df1["Birth Date"] = df1["Birth Date"].apply(format_date)
    df2["Birth Date"] = df2["Birth Date"].apply(format_date)

    merged = df1.merge(df2, on="Email", how="outer", suffixes=("1", "2"))
    merged.fillna("", inplace=True)
    merged["Differences"] = merged.apply(check_differences, axis=1)

    def get_first_name(row):
        if row["First Name"]:
            return row["First Name"]
        return row["Full Name"].split()[0] if row["Full Name"] else ""

    def get_last_name(row):
        if row["Last Name"]:
            return row["Last Name"]
        return row["Full Name"].split()[-1] if row["Full Name"] else ""

    def get_phone(row):
        return row["Phone1"] if row["Phone1"] else row["Phone2"]

    def get_birthdate(row):
        return row["Birth Date1"] if row["Birth Date1"] else row["Birth Date2"]

    output = pd.DataFrame()
    output["First name"] = merged.apply(get_first_name, axis=1)
    output["Last name"] = merged.apply(get_last_name, axis=1)
    output["Email"] = merged["Email"]
    output["Phone (US)"] = merged.apply(get_phone, axis=1)
    output["Gender"] = ""
    output["Birthdate (MM/DD/YYYY)"] = merged.apply(get_birthdate, axis=1)
    output["Address"] = merged.get("Address", "")
    output["City"] = merged.get("City", "")
    output["State"] = merged.get("State", "")
    output["ZIP Code"] = merged.get("ZIP", "")
    output["Note"] = merged.get("Notes", "")
    output["Processed by automatic migration?"] = "No"
    output["Last automatic migration error"] = ""

    output.to_csv(output_file, index=False)
    print("Report successfully generated:", output_file)

# Example usage:
process("input1.csv", "input2.csv", "final_report.csv")
