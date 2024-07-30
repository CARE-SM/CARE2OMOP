from utils import DataTransformation
import pandas as pd
import yaml
    
class CARE2OMOP:
    
        # Initialize data transformation

    def __init__(self, configuration_file):
        
        workflow = DataTransformation(configuration_file)

        person_header = ["person_id","gender_concept_id", "year_of_birth", "month_of_birth", "day_of_birth", "birth_datetime", "race_concept_id", "ethnicity_concept_id", "location_id", "provider_id", "care_site_id", "person_source_value", "gender_source_value", "gender_source_concept_id", "race_source_value", "race_source_concept_id", "ethnicity_source_value", "ethnicity_source_concept_id"]
        condition_header = ["condition_occurrence_id","person_id","condition_concept_id","condition_start_date","condition_start_datetime","condition_end_date","condition_end_datetime","condition_type_concept_id","condition_status_concept_id","stop_reason","provider_id","visit_occurrence_id","visit_detail_id","condition_source_value","condition_source_concept_id","condition_status_source_value"]
        visit_header = ["visit_occurrence_id","person_id","visit_concept_id","visit_start_date","visit_start_datetime","visit_end_time","visit_end_datetime","visit_type_concept_id","provider_id","care_site_id","visit_source_value","visit_source_concept_id","admitting_source_concept_id","admitting_source_value","discharge_to_concept_id","discharge_to_source_value","preceding_occurrence_id"]
        measurement_header = ["measurement_id","person_id","measurement_concept_id","measurement_date","measurement_datetime","measurement_time","measurement_type_concept_id","operator_concept_id","value_as_number","value_as_concept_id","unit_concept_id","range_low","range_high","provider_id","visit_occurrence_id","visit_detail_id","measurement_source_value","measurement_source_concept_id","unit_source_value","value_source_value"]
        death_header = ["person_id","death_date","death_datetime","death_type_concept_id","cause_concept_id","cause_source_value","cause_source_concept_id"]
        procedure_header = ["procedure_occurrence_id", "person_id", "procedure_concept_id", "procedure_date", "procedure_datetime", "procedure_end_date", "procedure_end_datetime", "procedure_type_concept_id", "modifier_concept_id", "quantity", "provider_id", "visit_occurrence_id", "visit_detail_id", "procedure_source_value", "procedure_source_concept_id", "modifier_source_value" ]
        observation_header=["observation_id","person_id","observation_concept_id","observation_date","observation_datetime","observation_type_concept_id","value_as_number","value_as_string","value_as_concept_id","qualifier_concept_id","unit_concept_id","provider_id","visit_occurrence_id","visit_detail_id","observation_source_value","observation_source_concept_id","unit_source_value","qualifier_source_value"]
        drug_header=["drug_exposure_id","person_id","drug_concept_id","drug_exposure_start_date","drug_exposure_start_datetime","drug_exposure_end_date","drug_exposure_end_datetime","verbatim_end_date","drug_type_concept_id","stop_reason","refills","quantity","days_supply","sig","route_concept_id","lot_number","provider_id","visit_occurrence_id","visit_detail_id","drug_source_value","drug_source_concept_id","route_source_value","dose_unit_source_value"]
        observation_period_header = ["person_id", "observation_period_start_date", "observation_period_end_date", "period_type_concept_id"]

        # PERSON
        extracted_table_person = workflow.extract_table("PERSON")
        transformed_table_person = workflow.table_person_transformation(extracted_table_person)
        transformed_table_person.to_csv("data/PERSON.csv", index = False, header=True)
        print("PERSON table have been created")

        # DEATH
        extracted_table_death = workflow.extract_table("DEATH")
        transformed_table_death = workflow.table_death_transformation(extracted_table_death)
        # selected_cols = [col for col in transformed_table_death.columns if col in death_header]
        # transformed_table_death = pd.DataFrame(transformed_table_death, columns=selected_cols)
        transformed_table_death.to_csv("data/DEATH.csv", index = False, header=True)
        print("DEATH table have been created")

        extracted_table_observation_period = workflow.extract_table("OBSERVATION-PERIOD")
        transformed_table_observation_period = workflow.table_observation_period_transformation(extracted_table_observation_period)
        transformed_table_observation_period.to_csv("data/OBSERVATION_PERIOD.csv", index = False, header=True)
        print("OBSERVATION_PERIOD table have been created")

        # CONDITION
        extracted_table_condition = workflow.extract_table("CONDITION")
        transformed_table_condition = workflow.table_condition_transformation(extracted_table_condition)
        selected_cols = [col for col in transformed_table_condition.columns if col in condition_header]
        resulting_transformed_table_condition = pd.DataFrame(transformed_table_condition, columns=selected_cols)
        resulting_transformed_table_condition.to_csv("data/CONDITION.csv", index = False, header=True)
        print("CONDITION table have been created")

        # MEASUREMENT
        extracted_table_measurement = workflow.extract_table("MEASUREMENT")
        transformed_table_measurement = workflow.table_measurement_transformation(extracted_table_measurement)
        selected_cols = [col for col in transformed_table_measurement.columns if col in measurement_header]
        resulting_transformed_table_measurement = pd.DataFrame(transformed_table_measurement, columns=selected_cols)
        resulting_transformed_table_measurement.to_csv("data/MEASUREMENT.csv", index = False, header=True)
        print("MEASUREMENT table have been created")

        # OBSERVATION
        extracted_table_observation = workflow.extract_table("OBSERVATION_")
        transformed_table_observation = workflow.table_observation_transformation(extracted_table_observation)
        selected_cols = [col for col in transformed_table_observation.columns if col in observation_header]
        resulting_transformed_table_observation = pd.DataFrame(transformed_table_observation, columns=selected_cols)
        resulting_transformed_table_observation.to_csv("data/OBSERVATION.csv", index = False, header=True)
        print("OBSERVATION table have been created")

        # PROCEDURE_OCURRENCE
        extracted_table_procedure = workflow.extract_table("PROCEDURE")
        transformed_table_procedure = workflow.table_procedure_transformation(extracted_table_procedure)
        selected_cols = [col for col in transformed_table_procedure.columns if col in procedure_header]
        resulting_transformed_table_procedure = pd.DataFrame(transformed_table_procedure, columns=selected_cols)
        resulting_transformed_table_procedure.to_csv("data/PROCEDURE_OCURRENCE.csv", index = False, header=True)
        print("PROCEDURE_OCURRENCE table have been created")

        # DRUG_EXPOSE
        extracted_table_drug = workflow.extract_table("DRUG")
        transformed_table_drug = workflow.table_drug_transformation(extracted_table_drug)
        selected_cols = [col for col in transformed_table_drug.columns if col in drug_header]
        resulting_transformed_table_drug = pd.DataFrame(transformed_table_drug, columns=selected_cols)
        resulting_transformed_table_drug.to_csv("data/DRUG_EXPOSE.csv", index = False, header=True)
        print("DRUG_EXPOSE table have been created")        

        # VISIT

        resulting_tranformed_table_visit = pd.DataFrame()

        selected_cols = [col for col in transformed_table_condition.columns if col in visit_header]
        resulting_visit_from_condition = pd.DataFrame(transformed_table_condition, columns=selected_cols)
        resulting_tranformed_table_visit = pd.concat([resulting_tranformed_table_visit, resulting_visit_from_condition])
        resulting_tranformed_table_visit = resulting_tranformed_table_visit.reset_index(drop=True)

        selected_cols = [col for col in transformed_table_measurement.columns if col in visit_header]
        resulting_visit_from_measurement = pd.DataFrame(transformed_table_measurement, columns=selected_cols)
        resulting_tranformed_table_visit = pd.concat([resulting_tranformed_table_visit, resulting_visit_from_measurement])
        resulting_tranformed_table_visit = resulting_tranformed_table_visit.reset_index(drop=True)

        selected_cols = [col for col in transformed_table_observation.columns if col in visit_header]
        resulting_visit_from_observation = pd.DataFrame(transformed_table_observation, columns=selected_cols)
        resulting_tranformed_table_visit = pd.concat([resulting_tranformed_table_visit, resulting_visit_from_observation])
        resulting_tranformed_table_visit = resulting_tranformed_table_visit.reset_index(drop=True)

        selected_cols = [col for col in transformed_table_procedure.columns if col in visit_header]
        resulting_visit_from_procedure = pd.DataFrame(transformed_table_procedure, columns=selected_cols)
        resulting_tranformed_table_visit = pd.concat([resulting_tranformed_table_visit, resulting_visit_from_procedure])
        resulting_tranformed_table_visit = resulting_tranformed_table_visit.reset_index(drop=True)

        selected_cols = [col for col in transformed_table_drug.columns if col in visit_header]
        resulting_visit_from_drug = pd.DataFrame(transformed_table_drug, columns=selected_cols)
        resulting_tranformed_table_visit = pd.concat([resulting_tranformed_table_visit, resulting_visit_from_drug])
        resulting_tranformed_table_visit = resulting_tranformed_table_visit.reset_index(drop=True)
        resulting_tranformed_table_visit.to_csv("data/VISIT.csv", index = False, header=True)
        print("VISIT table have been created")
        
   # Import configuration file
with open("configuration.yaml") as file:
    configuration_file = yaml.load(file, Loader=yaml.FullLoader)     
    
test = CARE2OMOP(configuration_file)