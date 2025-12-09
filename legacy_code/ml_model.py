import pandas as pd
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, classification_report

# --- 1. CONNECT TO DATABASE ---
# Use the same password you fixed earlier
DB_CONNECTION = 'postgresql://postgres:password@localhost:5432/microbiome_db'
engine = create_engine(DB_CONNECTION)

print("Loading data from SQL...")

# --- 2. SQL QUERY FOR FEATURE ENGINEERING ---
# We filter out 'Unknown' history to make the model cleaner
query = """
SELECT 
    age, 
    sex, 
    antibiotic_history, 
    shannon_entropy 
FROM samples 
WHERE antibiotic_history NOT IN ('Unspecified', 'Unknown') 
  AND sex IN ('male', 'female')
"""

df = pd.read_sql(query, engine)

# --- 3. CREATE TARGET VARIABLE ---
# We define "High Diversity" as being above the median
median_diversity = df['shannon_entropy'].median()
df['target_high_diversity'] = (df['shannon_entropy'] > median_diversity).astype(int)

print(f"Dataset Size: {len(df)} samples")
print(f"Median Diversity: {median_diversity:.2f}")

# --- 4. PREPARE FEATURES (ONE-HOT ENCODING) ---
# Convert categories (Sex, Antibiotics) into numeric 1/0 columns
# drop_first=True avoids multicollinearity (e.g., if not Male, it's Female)
X = pd.get_dummies(df[['age', 'sex', 'antibiotic_history']], drop_first=True)
y = df['target_high_diversity']

# --- 5. TRAIN MODEL ---
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

model = LogisticRegression(max_iter=1000)
model.fit(X_train, y_train)

# --- 6. EVALUATE ---
predictions = model.predict(X_test)
accuracy = accuracy_score(y_test, predictions)

print(f"\nModel Accuracy: {accuracy:.2%}")
print("\n--- Feature Importance (What affects diversity?) ---")

# Match coefficients to column names to see what drives the prediction
coefficients = pd.DataFrame({
    'Feature': X.columns,
    'Weight': model.coef_[0]
}).sort_values(by='Weight', ascending=True)

print(coefficients)

print("\nInterpretation:")
print("Negative Weight = Lowers Diversity (Bad)")
print("Positive Weight = Increases Diversity (Good)")