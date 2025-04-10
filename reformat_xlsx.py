import pandas as pd
import os
import re
from fire import Fire

def extract_tables_from_excel(excel_file, measure_data_type):
    """
    Extract tables from an Excel file where each table has a county name header
    followed by a data table with year, period, label, and observed value.
    
    Args:
        excel_file (str): Path to the Excel file
        
    Returns:
        dict: Dictionary with county names as keys and dataframes as values
    """
    # Read the entire Excel file
    xl = pd.ExcelFile(excel_file)
    
    # Dictionary to store county data
    county_data = {}
    
    # Process each sheet in the Excel file
    for sheet_name in xl.sheet_names:
        # Read the sheet into a DataFrame
        df = pd.read_excel(excel_file, sheet_name=sheet_name, header=None)
        
        # Find rows with county names (assumption: county names contain "County")
        county_rows = []
        is_measure_type = False

        for i, row in df.iterrows():
            # Convert row to string and check if it contains "County"
            row_str = ' '.join(str(x) for x in row.dropna().tolist())

            if row_str.strip().lower().endswith(measure_data_type.strip()):
                is_measure_type = True
                continue

            if "County" in row_str:
                county_name = row_str.strip()
                county_rows.append((i, county_name))

        if not is_measure_type:
            print(f"Skipping sheet '{sheet_name}' because it doesn't contain {measure_data_type}")
            continue

        # Process each county section
        for i, (county_row_idx, county_name) in enumerate(county_rows):
            # Determine the end of this county's data (either next county or end of sheet)
            end_row = county_rows[i+1][0] if i < len(county_rows) - 1 else df.shape[0]
            
            # Look for the header row (containing "Year", "Period", "Label", "Value")
            header_row = None
            for j in range(county_row_idx, end_row):
                row_values = [str(x).lower() for x in df.iloc[j].dropna().tolist()]
                if "year" in row_values and "period" in row_values and ("label" in row_values or "value" in row_values):
                    header_row = j
                    break
            
            if header_row is not None:
                # Extract the data starting from the row after the header
                data_start = header_row + 1
                data_end = end_row
                
                # Create a dataframe from the data rows
                county_df = pd.DataFrame(df.iloc[data_start:data_end].values)
                county_df = county_df.dropna(how='any', axis=1)                

                # Set the column names
                county_df.columns = ['year', 'month_id', 'time_name', f'obs_value_{county_name}'] 
                county_data[county_name] = county_df   
    return county_data

def process(excel_file: str, measure_data_type: str, output_dir: str = "output"):    
    # Extract tables
    county_data = extract_tables_from_excel(excel_file, measure_data_type)

    measure_data_slug = measure_data_type.replace(" ", "_").lower().strip()
    
    # Create an aggregated dataframe
    aggregated_data = pd.DataFrame()
    
    for county, data in county_data.items():
        if "city" in county.lower():
            print(f"Skipping county: {county} because it might be a city")
            continue
        if aggregated_data.empty:
            aggregated_data = data
        else:
            aggregated_data = pd.merge(aggregated_data, data, on=['year', 'month_id', 'time_name'], how='outer')
    
    # Print summary of aggregated data
    print(f"Total rows in aggregated data: {len(aggregated_data)}")
    print(f"Counties included: {len(county_data)}")
    print(aggregated_data.head())
    
    # Save aggregated data
    os.makedirs(output_dir, exist_ok=True)
    
    # Save the aggregated data to a single file
    aggregated_file = os.path.join(output_dir, excel_file.split(".")[0] + f"_{measure_data_slug}_aggregated.csv")
    aggregated_data.to_csv(aggregated_file, index=False)
    print(f"Saved aggregated data to {aggregated_file}")

if __name__ == "__main__":
    Fire(process)