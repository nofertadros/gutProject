import pandas as pd
from scipy.stats import ttest_ind
from sqlalchemy import create_engine

# 1. Connect
# (Make sure your password is correct)
engine = create_engine('postgresql://postgres:password@localhost:5432/microbiome_db')

print("Running Statistical T-Test...")

# 2. Get the Two Groups
# FIX: Use exact string match (=) instead of LIKE (%) to avoid Python/SQL conflict
query_recent = "SELECT shannon_entropy FROM samples WHERE antibiotic_history = 'Month'"
query_healthy = "SELECT shannon_entropy FROM samples WHERE antibiotic_history = 'I have not taken antibiotics in the past year.'"

group_recent = pd.read_sql(query_recent, engine)
group_healthy = pd.read_sql(query_healthy, engine)

# 3. Run T-Test
# This calculates if the difference between the two groups is "real" or just luck
t_stat, p_val = ttest_ind(group_recent['shannon_entropy'], group_healthy['shannon_entropy'], equal_var=False)

# 4. Print Results
print(f"Sample Size (Recent Antibiotics): {len(group_recent)}")
print(f"Sample Size (Healthy Controls):   {len(group_healthy)}")
print(f"Mean Diversity (Recent):  {group_recent['shannon_entropy'].mean():.2f}")
print(f"Mean Diversity (Healthy): {group_healthy['shannon_entropy'].mean():.2f}")
print(f"\nP-Value: {p_val:.20f}") # Printing 20 decimal places to see the tiny number

if p_val < 0.05:
    print("\nCONCLUSION: The difference is STATISTICALLY SIGNIFICANT.")
    print("This proves the drop in diversity is not due to random chance.")
else:
    print("\nCONCLUSION: Not significant.")