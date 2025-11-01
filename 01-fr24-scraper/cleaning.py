import pandas as pd

# Read the CSV file
df = pd.read_csv('others1.csv')

# Drop rows where AIRLINE is 'AirAsia' or "T'way Air"
df = df[~df["AIRLINE"].isin(["AirAsia", "T'way Air"])]

# Save the cleaned dataframe to a new CSV file
output_path = "flights_cleaned2.csv"
df.to_csv(output_path, index=False)

# Get and print unique airlines from the cleaned dataset
unique_airlines = df["AIRLINE"].dropna().unique()

print(f"Cleaned CSV saved as: {output_path}\n")
print("Unique Airlines (after filtering):")
for airline in unique_airlines:
    print(airline)