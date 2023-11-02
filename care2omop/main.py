from utils import DataTransformation
import yaml
import pandas as pd

# Import configuration file
with open("configuration.yaml") as file:
    configuration_file = yaml.load(file, Loader=yaml.FullLoader)

# Initialize data transformation
workflow = DataTransformation(configuration_file)

person_header = ["person_id","gender_concept_id", "year_of_birth", "month_of_birth", "day_of_birth", "birth_datetime", "race_concept_id", "ethnicity_concept_id", "location_id", "provider_id", "care_site_id", "person_source_value", "gender_source_value", "gender_source_concept_id", "race_source_value", "race_source_concept_id", "ethnicity_source_value", "ethnicity_source_concept_id"]
condition_header = ["condition_occurrence_id","person_id","condition_concept_id","condition_start_date","condition_start_datetime","condition_end_date","condition_end_datetime","condition_type_concept_id","condition_status_concept_id","stop_reason","provider_id","visit_occurrence_id","visit_detail_id","condition_source_value","condition_source_concept_id","condition_status_source_value"]
visit_header = ["visit_occurrunce_id","person_id","visit_concept_id","visit_start_date","visit_start_datetime","visit_end_time","visit_end_datetime","visit_type_concept_id","provider_id","care_site_id","visit_source_value","visit_source_concept_id","admitting_source_concept_id","admitting_source_value","discharge_to_concept_id","discharge_to_source_value","preceding_visit_occurrence_id"]
measurement_header = ["measurement_id","person_id","measurement_concept_id","measurement_date","measurement_datetime","measurement_time","operator_concept_id","value_as_number","value_as_concept_id","unit_concept_id","range_low","range_high","provider_id","visit_occurrence_id","visit_detail_id","measurement_source_value","measurement_source_conept_id","unit_source_value","value_source_value"]
death_header = ["person_id","death_date","death_datetime","death_type_concept_id","cause_concept_id","cause_source_value","cause_source_concept_id"]
  

extracted_table_person = workflow.extract_table("PERSON", path= "templates/PERSON.mustache")
extracted_table_condition_visit = workflow.extract_table("CONDITION_VISIT", path= "templates/CONDITION_VISIT.mustache")
extracted_table_measurement_visit = workflow.extract_table("MEASUREMENT_VISIT", path= "templates/MEASUREMENT_VISIT.mustache")
extracted_table_death = workflow.extract_table("DEATH", path= "templates/DEATH.mustache")


# PERSON
transfomed_table_person = workflow.table_person_transformation(extracted_table_person)
transfomed_table_person.to_csv("data/PERSON.csv", index = False, header=True)
print("PERSON table have been created")


# CONDITION
transfomed_table_condition_visit = workflow.table_person_transformation(extracted_table_condition_visit)
selected_cols = [col for col in transfomed_table_condition_visit.columns if col in condition_header]
transfomed_table_condition_visit = pd.DataFrame(transfomed_table_condition_visit, columns=selected_cols)
transfomed_table_condition_visit.to_csv("data/CONDITION.csv", index = False, header=True)
print("CONDITION table have been created")


# MEASUREMENT
transfomed_table_measurement_visit = workflow.table_person_transformation(extracted_table_measurement_visit)
selected_cols = [col for col in transfomed_table_measurement_visit.columns if col in measurement_header]
transfomed_table_measurement_visit = pd.DataFrame(transfomed_table_measurement_visit, columns=selected_cols)
transfomed_table_measurement_visit.to_csv("data/MEASUREMENT.csv", index = False, header=True)
print("MEASUREMENT table have been created")

# VISIT
selected_cols = [col for col in transfomed_table_measurement_visit.columns if col in visit_header]
df_VISIT_M = pd.DataFrame(transfomed_table_measurement_visit, columns=selected_cols)

selected_cols = [col for col in transfomed_table_condition_visit.columns if col in visit_header]
df_VISIT_C = pd.DataFrame(transfomed_table_condition_visit, columns=selected_cols)

df_VISIT = pd.concat([df_VISIT_C, df_VISIT_M])
df_VISIT.to_csv("data/VISIT.csv", index = False, header=True)
print("VISIT table have been created")

# DEATH
transfomed_table_death = workflow.table_person_transformation(extracted_table_death)
transfomed_table_death.to_csv("data/DEATH.csv", index = False, header=True)
print("DEATH table have been created")
