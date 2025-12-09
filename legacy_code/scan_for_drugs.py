import pandas as pd

print("Scanning for Drug & Supplement columns...")

# Read just the header (first row) to see column names
try:
    df = pd.read_csv('data/ag-cleaned.txt', sep='\t', encoding='latin1', low_memory=False, nrows=5)
except FileNotFoundError:
    df = pd.read_csv('ag-cleaned.txt', sep='\t', encoding='latin1', low_memory=False, nrows=5)

# Keywords to look for
keywords = ['MEDICATION', 'ANTIBIOTIC', 'SUPPLEMENT', 'VITAMIN', 'PROBIOTIC', 'IBUPROFEN', 'ASPIRIN', 'TYLENOL', 'BIRTH_CONTROL']

found_cols = []

for col in df.columns:
    # Check if any keyword is inside the column name (Case Insensitive)
    for key in keywords:
        if key in col.upper():
            found_cols.append(col)
            break

print(f"\n--- Found {len(found_cols)} Potential Drug Columns ---")
for col in found_cols:
    print(f"- {col}")