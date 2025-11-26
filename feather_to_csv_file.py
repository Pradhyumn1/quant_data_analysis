import pandas as pd


input_file = "/Users/pradhyumnyadav/Desktop/multify/nifty50_processed/ADANIENT_2025-10-31.feather"

output_file = "BajajAuto_2025-10-31.csv"


df = pd.read_feather(input_file)

df.to_csv(output_file, index=False)

print("Conversion complete! CSV saved as:", output_file)
