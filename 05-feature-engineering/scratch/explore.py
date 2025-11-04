import pandas as pd

df = pd.read_csv("consolidated_flight_data_cleaned.csv")

# get unique airlines
unique_airlines = df['Airline'].dropna().unique()

# print each airline
print("Unique Airlines:")
for airline in unique_airlines:
    print(airline)

# or if you want to save them to a CSV
#pd.DataFrame({'Airline': unique_airlines}).to_csv("unique_airlines.csv", index=False)
