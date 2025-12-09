import pandas as pd
from scipy.stats import f_oneway
from sqlalchemy import create_engine

# 1. Connect
engine = create_engine('postgresql://postgres:password@localhost:5432/microbiome_db')

print("Fetching data for ANOVA...")

# 2. Get the Raw Data
# We need the raw lists of bacteria counts for every single patient in each group
query = """
SELECT 
    CASE 
        WHEN diet_type = 'Vegan' THEN 'Vegan'
        WHEN diet_type = 'Omnivore' AND red_meat_freq IN ('Rarely (less than once/week)', 'Occasionally (1-2 times/week)') THEN 'Moderate'
        WHEN diet_type = 'Omnivore' AND red_meat_freq IN ('Daily', 'Regularly (3-5 times/week)') THEN 'High_Meat'
        ELSE 'Other'
    END as diet_group,
    prevotella,
    bacteroides
FROM samples s
JOIN key_species k ON s.sample_id = k.sample_id
WHERE diet_type IN ('Vegan', 'Omnivore')
"""
df = pd.read_sql(query, engine)

# Filter out "Other"
df = df[df['diet_group'] != 'Other']

# 3. Separate the Groups
vegans = df[df['diet_group'] == 'Vegan']['prevotella']
moderate = df[df['diet_group'] == 'Moderate']['prevotella']
high_meat = df[df['diet_group'] == 'High_Meat']['prevotella']

print(f"\n--- Group Sizes ---")
print(f"Vegans: {len(vegans)}")
print(f"Moderate: {len(moderate)}")
print(f"High Meat: {len(high_meat)}")

# 4. Run ANOVA (Prevotella)
# This asks: "Is the variance BETWEEN groups larger than the variance WITHIN groups?"
f_stat, p_value = f_oneway(vegans, moderate, high_meat)

print(f"\n--- ANOVA Results (Prevotella) ---")
print(f"F-Statistic: {f_stat:.4f}")
print(f"P-Value:     {p_value:.10f}")

if p_value < 0.05:
    print("CONCLUSION: Statistically Significant! Diet determines Prevotella levels.")
else:
    print("CONCLUSION: No significant difference found.")

# 5. Run ANOVA (Bacteroides)
vegans_b = df[df['diet_group'] == 'Vegan']['bacteroides']
moderate_b = df[df['diet_group'] == 'Moderate']['bacteroides']
high_meat_b = df[df['diet_group'] == 'High_Meat']['bacteroides']

f_stat_b, p_value_b = f_oneway(vegans_b, moderate_b, high_meat_b)

print(f"\n--- ANOVA Results (Bacteroides) ---")
print(f"F-Statistic: {f_stat_b:.4f}")
print(f"P-Value:     {p_value_b:.10f}")