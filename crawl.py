import requests
import json
import pandas as pd
import random
import time

# Define URL pattern
BASE_URL = "https://cdi.seadatanet.org/report/{}/json"

# Define the category mapping
CATEGORY_MAPPING = {
    "name": ["Data set name", "Cruise name", "Alternative cruise name", "Station name", "Alternative station name"],
    "place": ["Latitude", "Longitude", "Sea regions", "MSFD areas"],
    "time": ["Start date", "Start time", "End date", "End time", "Cruise start date", "Station start date"],
    "description": ["Abstract", "Discipline", "Parameter groups", "Discovery parameter", "GEMET-INSPIRE themes"],
    "other_numeric": ["Data size", "Water depth (m)", "Minimum instrument depth (m)", "Maximum instrument depth (m)"]
}

# Function to categorize a key
def categorize_key(key):
    key_lower = key.lower()
    for category, keywords in CATEGORY_MAPPING.items():
        if any(keyword.lower() in key_lower for keyword in keywords):
            return category
    return None  # If no category match is found

# Store extracted data
extracted_data = []
file_records = {}  # Stores extracted values per file for random pairing

# Iterate over URLs 1-N
for val in range(1, 21):
    url = BASE_URL.format(val)
    print(f"Fetching: {url}")

    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()  # Convert JSON to Python dictionary

            file_name = f"report_{val}.json"
            file_name = f"{url}"
            file_records[file_name] = []

            # Iterate over JSON structure
            for section, values in data.items():
                if isinstance(values, dict):  # Ensure it's a dictionary
                    for key, value in values.items():
                        category = categorize_key(key)
                        if category:
                            extracted_entry = {
                                "File": file_name,
                                "Category": category,
                                "Key": key,
                                "Value": str(value)  # Convert all values to string for uniformity
                            }
                            extracted_data.append(extracted_entry)
                            file_records[file_name].append(str(value))  # Store only the values

        else:
            print(f"Failed to retrieve data from {url}. Status Code: {response.status_code}")

        # Pause to avoid overwhelming the server
        time.sleep(1)

    except Exception as e:
        print(f"Error fetching {url}: {e}")

# Pair values randomly
random_pairs = []
file_names = list(file_records.keys())

for file1 in file_names:
    if not file_records[file1]:
        continue  # Skip empty files

    # Select a random value from file1
    value1 = random.choice(file_records[file1])

    # Find another file to pair with
    file2 = random.choice([f for f in file_names if f != file1 and file_records[f]])
    value2 = random.choice(file_records[file2])

    # Store the paired values
    random_pairs.append({
        "Pair 1 (Value)": value1,
        "Pair 2 (Value)": value2,
        "Pair 1 File Name": file1,
        "Pair 2 File Name": file2
    })

# Save extracted values
df_extracted = pd.DataFrame(extracted_data)
df_extracted.to_csv("extracted_values.csv", index=False)
print("Extracted values saved to extracted_values.csv")

# Save random value pairs
df_pairs = pd.DataFrame(random_pairs)
df_pairs.to_csv("random_value_pairs.csv", index=False)
print("Random value pairs saved to random_value_pairs.csv")
