import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from sqlalchemy import create_engine

# 1. Connect
engine = create_engine('postgresql://postgres:password@localhost:5432/microbiome_db')

# 2. Get the Data
query = """
SELECT antibiotic_history, shannon_entropy 
FROM samples 
WHERE antibiotic_history IN ('Week', 'Month', 'Year', 'I have not taken antibiotics in the past year.')
"""
df = pd.read_sql(query, engine)

# 3. Clean Labels for the Plot
# Shorten the long label to make the chart readable
df['antibiotic_history'] = df['antibiotic_history'].replace(
    'I have not taken antibiotics in the past year.', 'None (>1 Yr)'
)

# 4. Create Violin Plot
plt.figure(figsize=(12, 6))
sns.violinplot(x='antibiotic_history', y='shannon_entropy', data=df, 
               order=['Week', 'Month', 'Year', 'None (>1 Yr)'],
               palette='muted')

plt.title('Impact of Recent Antibiotic Use on Gut Microbiome Diversity')
plt.ylabel('Shannon Diversity Index')
plt.xlabel('Time Since Last Antibiotic Dose')
plt.grid(axis='y', linestyle='--', alpha=0.7)

# 5. Save
plt.savefig('antibiotic_impact.png')
print("Chart saved as 'antibiotic_impact.png'. Open it to see the distribution!")