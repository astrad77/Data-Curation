import pandas as pd

# Load prices dataset
df_prices = pd.read_csv("electricity_prices.csv", skiprows = 4)  # Price data

df_prices.rename(columns={"all sectors cents per kilowatthour": "price_all", "residential cents per kilowatthour":"price_residential", "commercial cents per kilowatthour":"price_commercial", "industrial cents per kilowatthour":"price_industrial"}, inplace=True)

#Define renewable sources
renewable_sources = ["Wind", "Solar", "Hydroelectric", "Geothermal", "Other Biomass"]

#Read generation data from Excel file
xls = pd.ExcelFile("generation_monthly.xlsx")
# Initialize an empty list to store yearly data
yearly_data = []

# Process each sheet
for i, sheet in enumerate(xls.sheet_names):
    if "Notes" in sheet or "Preliminary" in sheet:  # Skip metadata/preliminary sheets
        continue

    # Determine how many rows to skip
    skip_rows = 0 if i < 5 else 4  # First 5 sheets have different formatting

    # Load the sheet
    df = pd.read_excel(xls, sheet_name=sheet, skiprows=skip_rows)
    
    # Standardize column names (strip spaces and remove newlines)
    df.columns = df.columns.str.strip().str.replace("\n", " ")

    # Filter only renewable sources
    df_renewable = df[df["ENERGY SOURCE"].isin(renewable_sources)]
    
    # Check if this sheet contains two years
    if i < 5:  # First 5 sheets contain two years
        if "YEAR" in df.columns:
            for year in df["YEAR"].unique():  # Process each year separately
                df_year = df_renewable[df_renewable["YEAR"] == year]
                
                # Compute the yearly average generation per month
                df_avg_generation = df_year.groupby("MONTH", as_index=False)["GENERATION (Megawatthours)"].sum()
                
                # Rename column for clarity
                df_avg_generation.rename(columns={"GENERATION (Megawatthours)": "Total_Renewable_Generation_MWh"}, inplace=True)
                
                # Add the year column
                df_avg_generation["Year"] = year
                
                # Append to list
                yearly_data.append(df_avg_generation)
    else:  # Sheets after the first five contain only one year
        year = int(sheet)  # Extract year from sheet name
        df_avg_generation = df_renewable.groupby("MONTH", as_index=False)["GENERATION (Megawatthours)"].sum()
        df_avg_generation.rename(columns={"GENERATION (Megawatthours)": "Total_Renewable_Generation_MWh"}, inplace=True)
        df_avg_generation["Year"] = year
        yearly_data.append(df_avg_generation)

# Combine all years into a single dataset
df_generation_final = pd.concat(yearly_data, ignore_index=True)
df_generation_final.rename(columns={"MONTH": "Month"}, inplace=True)


# Save to CSV for reference
df_generation_final.to_csv("renewable_generation.csv", index=False)


df_prices["Year"] = df_prices["Month"].apply(lambda x: int(x.split(" ")[1]))  # Extract year
df_prices["Month"] = df_prices["Month"].apply(lambda x: pd.to_datetime(x.split(" ")[0], format='%b').month)  # Convert month name to number

# Merge the two datasets on Year and Month
df_combined = pd.merge(df_prices, df_generation_final, on=["Year", "Month"], how="inner")

# Save the final dataset
df_combined.to_csv("final_electricity_data.csv", index=False)


