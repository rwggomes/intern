import pandas as pd 
import taxjar 
from taxjar.exceptions import TaxJarError 
import os
from dotenv import load_dotenv 

# Load environment variables from .env
load_dotenv()
TAXJAR_API_KEY = os.getenv("TAXJAR_API_KEY")

# Check if the API key is available
if not TAXJAR_API_KEY:
    raise ValueError("TaxJar API Key is missing. Please make sure it's set in the .env file.")

# Initialize the TaxJar client for the sandbox API
client = taxjar.Client(api_key=TAXJAR_API_KEY, api_url='https://api.sandbox.taxjar.com')

print("API key loaded successfully.")

# Read the input CSV file
input_csv_path = 'example_medspa_data.csv'
try:
    medspa_data = pd.read_csv(input_csv_path)
    print(f"Input CSV loaded successfully from {input_csv_path}.")
except FileNotFoundError:
    print(f"Error: Could not find the file at {input_csv_path}. Exiting.")
    exit(1)

# Ensure all the necessary columns are in the CSV
required_columns = ['medspa_name', 'medspa_address_1', 'medspa_city', 'medspa_state', 'medspa_zip']
for col in required_columns:
    if col not in medspa_data.columns:
        raise ValueError(f"Error: Missing required column: {col}")

# Function to get the sales tax rate for a given address
def get_sales_tax_rate(address_1, city, state, zip_code):
    if not address_1 or not city or not state or not zip_code:
        print(f"Skipping row because of missing fields: {address_1}, {city}, {state}, {zip_code}")
        return None

    try:
        # Convert zip code to string in case it's an int
        zip_code = str(zip_code)
        print(f"Fetching tax rate for ZIP: {zip_code} in {city}, {state}")
        
        # Call TaxJar API
        tax_rate_response = client.rates_for_location(zip_code, {
            'street': address_1,
            'city': city,
            'state': state
        })
        
        # Return the combined sales tax rate
        return tax_rate_response.combined_rate
    
    except TaxJarError as e:
        print(f"Error fetching tax rate for ZIP {zip_code}: {e}")
        return None

# Function to process each row in the CSV
def process_row(row):
    try:
        return get_sales_tax_rate(row['medspa_address_1'], row['medspa_city'], row['medspa_state'], row['medspa_zip'])
    except Exception as e:
        print(f"Error processing row {row['medspa_name']}: {e}")
        return None

# Apply the function to each row to calculate the sales tax rate
print("Processing each row in the input CSV.")
medspa_data['medspa_sales_tax_rate'] = medspa_data.apply(process_row, axis=1)

# Save the updated CSV with the sales tax rates
output_csv_path = 'updated_medspa_data.csv'
medspa_data.to_csv(output_csv_path, index=False)

print(f"Updated CSV saved at {output_csv_path}.") 
