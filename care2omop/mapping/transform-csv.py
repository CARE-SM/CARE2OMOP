import pandas as pd

# Read the tab-separated file
df = pd.read_csv("CONCEPT.csv", sep="\t")

# Drop unwanted columns
df = df.drop(columns=[
    "concept_name",
    "valid_start_date",
    "valid_end_date",
    "invalid_reason",
    "domain_id",
    "standard_concept",
    "concept_class_id"
])

# Remove unneccesary rows
df = df[df["vocabulary_id"] != "CDM"]
df = df[df["vocabulary_id"] != "OSM"]

df = df[df["concept_code"].astype(str).str.len() <= 20] # Removing row Blank nodes rather than concept_code

# Save as comma-separated CSV
df.to_csv("snomed_to_athena.csv", index=False)