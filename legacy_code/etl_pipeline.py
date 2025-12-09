import pandas as pd
from sqlalchemy import create_engine
import numpy as np

# --- CONFIGURATION ---
# UPDATE THIS with your actual PostgreSQL password and username
DB_USER = 'postgres'
DB_PASS = 'password'  # <--- CHANGE THIS
DB_NAME = 'microbiome_db'
DB_PORT = '5432'
db_connection_str = f'postgresql://{DB_USER}:{DB_PASS}@localhost:{DB_PORT}/{DB_NAME}'

def run_etl():
    print("--- Starting ETL Pipeline ---")

    # ==========================================
    # STEP 1: LOAD AND CLEAN METADATA
    # ==========================================
    print("Step 1: Loading Metadata (ag-cleaned.txt)...")
    
    # 'latin1' encoding is required for AGP files
    df_meta = pd.read_csv('ag-cleaned.txt', sep='\t', encoding='latin1', low_memory=False)
    
    # 1.1 Rename the ID column (Usually '#SampleID' or 'SampleID')
    # We grab the first column dynamically to be safe
    id_col = df_meta.columns[0]
    df_meta.rename(columns={id_col: 'sample_id'}, inplace=True)
    
    # 1.2 Identify the Medication Text Column
    # AGP usually puts free-text meds in 'VIOLATION' or 'subset_medication'
    med_text_col = None
    if 'VIOLATION' in df_meta.columns:
        med_text_col = 'VIOLATION'
    elif 'subset_medication' in df_meta.columns:
        med_text_col = 'subset_medication'
    
    # 1.3 Select Columns of Interest
    # We keep SampleID, Age, Sex, BMI, Country, Antibiotic History, and the Med Text
    cols_to_keep = ['sample_id', 'AGE_YEARS', 'SEX', 'BMI', 'COUNTRY', 'ANTIBIOTIC_HISTORY']
    
    if med_text_col:
        cols_to_keep.append(med_text_col)
        print(f"Found medication text column: {med_text_col}")
    else:
        print("WARNING: No free-text medication column found (VIOLATION). Polypharmacy parsing may be limited.")

    # Filter columns that actually exist in the file
    existing_cols = [c for c in cols_to_keep if c in df_meta.columns]
    df_clean = df_meta[existing_cols].copy()
    
    # 1.4 Standardize Column Names
    rename_map = {
        'AGE_YEARS': 'age', 
        'SEX': 'sex', 
        'BMI': 'bmi', 
        'COUNTRY': 'country',
        'ANTIBIOTIC_HISTORY': 'antibiotic_history'
    }
    if med_text_col:
        rename_map[med_text_col] = 'med_text'
        
    df_clean.rename(columns=rename_map, inplace=True)

    # 1.5 Data Cleaning
    # Filter for USA only (optional, but good for consistency)
    if 'country' in df_clean.columns:
        df_clean = df_clean[df_clean['country'] == 'USA']
    
    # Convert Age to numeric
    if 'age' in df_clean.columns:
        df_clean['age'] = pd.to_numeric(df_clean['age'], errors='coerce')
        df_clean.dropna(subset=['age'], inplace=True)

    print(f"Metadata processed: {len(df_clean)} samples.")

    # ==========================================
    # STEP 2: LOAD AND PARSE DIVERSITY MATRIX
    # ==========================================
    print("Step 2: Loading Diversity Matrix (shannon.txt)...")
    
    # Read the raw file
    df_alpha_raw = pd.read_csv('shannon.txt', sep='\t')
    
    # Find the depth column (e.g., "sequences per sample")
    depth_cols = [c for c in df_alpha_raw.columns if 'sequences' in c.lower()]
    if not depth_cols:
        raise ValueError("Could not find 'sequences per sample' column in shannon.txt")
    depth_col = depth_cols[0]
    
    # Filter for 10,000 reads depth
    df_alpha_10k = df_alpha_raw[df_alpha_raw[depth_col] == 10000].copy()
    
    if df_alpha_10k.empty:
        raise ValueError("No data found for depth 10,000. Check the file.")

    # Drop metadata columns (first 3 columns: path, depth, iteration) to get only sample data
    # We transpose so Samples become Rows
    data_only = df_alpha_10k.iloc[:, 3:]
    
    # Calculate average across iterations (if multiple exist)
    avg_diversity = data_only.mean(axis=0)
    
    # Convert Series to DataFrame
    df_alpha = avg_diversity.to_frame(name='shannon_entropy')
    df_alpha.index.name = 'sample_id'
    df_alpha.reset_index(inplace=True)
    
    # Clean Sample IDs (ensure string format)
    df_alpha['sample_id'] = df_alpha['sample_id'].astype(str)
    
    print(f"Diversity data processed: {len(df_alpha)} samples.")

    # ==========================================
    # STEP 3: MERGE DATASETS
    # ==========================================
    print("Step 3: Merging Metadata and Diversity...")
    
    # Ensure ID types match
    df_clean['sample_id'] = df_clean['sample_id'].astype(str)
    
    df_final = pd.merge(df_clean, df_alpha, on='sample_id', how='inner')
    
    print(f"Merged Dataset: {len(df_final)} samples (Intersection of Metadata & Diversity).")

    # ==========================================
    # STEP 4: PARSE MEDICATIONS (POLYPHARMACY)
    # ==========================================
    print("Step 4: Parsing Medications against Dictionary...")
    
    try:
        df_drugs = pd.read_csv('drug_mapping.csv')
        patient_meds_list = []
        
        if 'med_text' in df_final.columns:
            # Iterate through samples
            for index, row in df_final.iterrows():
                if pd.isna(row['med_text']):
                    continue
                
                # Normalize text to lower case
                user_text = str(row['med_text']).lower()
                sample_id = row['sample_id']
                
                # Check against every keyword in dictionary
                for _, drug_row in df_drugs.iterrows():
                    keyword = str(drug_row['keyword']).lower()
                    if keyword in user_text:
                        patient_meds_list.append({
                            'sample_id': sample_id,
                            'generic_name': drug_row['generic_name'],
                            'drug_class': drug_row['drug_class']
                        })
            
            df_patient_meds = pd.DataFrame(patient_meds_list)
            if not df_patient_meds.empty:
                df_patient_meds.drop_duplicates(inplace=True)
                print(f"Found {len(df_patient_meds)} medication matches.")
            else:
                print("No medication matches found. Check your drug_mapping.csv keywords.")
                df_patient_meds = pd.DataFrame(columns=['sample_id', 'generic_name', 'drug_class'])
        else:
            print("Skipping text parsing (No 'med_text' column).")
            df_patient_meds = pd.DataFrame(columns=['sample_id', 'generic_name', 'drug_class'])

    except FileNotFoundError:
        print("drug_mapping.csv not found. Skipping medication parsing.")
        df_patient_meds = pd.DataFrame(columns=['sample_id', 'generic_name', 'drug_class'])

    # ==========================================
    # STEP 5: UPLOAD TO SQL
    # ==========================================
    print("Step 5: Uploading to PostgreSQL...")
    
    engine = create_engine(db_connection_str)
    
    # Upload Samples Table
    # Drop med_text before upload to keep table clean (optional)
    cols_for_sql = [c for c in df_final.columns if c != 'med_text']
    df_final[cols_for_sql].to_sql('samples', engine, if_exists='replace', index=False)
    
    # Upload Medications Table
    df_patient_meds.to_sql('patient_medications', engine, if_exists='replace', index=False)
    
    print("--- ETL Complete! Tables 'samples' and 'patient_medications' created. ---")

if __name__ == "__main__":
    run_etl()