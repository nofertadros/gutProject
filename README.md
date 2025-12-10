# Polypharmacy & Metabolic Drivers of the Human Microbiome

**A Data Science Analysis of the American Gut Project**

## Project Overview

This project analyzes 12,562 human samples to determine the impact of antibiotics, diet, polypharmacy, and BMI on gut microbiome diversity. It utilizes a PostgreSQL database (Snowflake Schema), Gradient Boosting Classifiers, and Recommendation Algorithms.

## Key Findings

1. **BMI** is the strongest predictor of gut health (96% Model Accuracy).
2. **Antibiotics** significantly reduce *Faecalibacterium* (Anti-inflammatory bacteria).
3. **Obesity** significantly reduces *Akkermansia* (Gut barrier protector).
4. **Polypharmacy**: "Acne Medication" and "Probiotics" showed no significant impact on diversity, while **Multivitamins** showed a reverse-correlation with health.
5. **Geography**: The "Western Diet" effect overrides individual choices (e.g., Vegans vs. Meat Eaters showed similar diversity ratios).

## Folder Structure

* `data/`: Raw Source Data (Metadata & BIOM files).
* `results/`: Generated visualizations (Violin plots, Feature Importance).
* `legacy_code/`: Archived scripts from early development phases.

## How to Run the Pipeline

**Prerequisites:** PostgreSQL must be installed and running.

1. **Install Dependencies:**
   `pip install -r requirements.txt`

2. **Initialize Database:**
   `sudo -u postgres psql -c "CREATE DATABASE microbiome_db;"`
   `sudo -u postgres psql -d microbiome_db -f schema_creation.sql`

3. **Run the ETL (Data Ingestion):**
   `python etl_advanced.py` (Loads Demographics, Diet, and Drugs)

4. **Extract Biology Data:**
   `python extract_species.py` (Parses BIOM file)
   `python load_species.py` (Loads Bacteria to SQL)

5. **Run Analysis & Modeling:**
   
   * `python ml_gradient_boost.py` (Predictive Model)
   * `python stats_new_targets.py` (Statistical Validation)
   * `python visualize_targeted_questions.py` (Vegan & Probiotic Analysis)
   * `python visualize_lifestyle.py` (Vitamin & Acne Analysis)

6. **Run the Product (Recommender):**
   * `python recommender.py` (Finds your "Healthy Twin")
   * `python recommender_visual.py` (Finds your "Healthy Twin on a GUI")

