import chevron
from auth import ServerConnection
import yaml
import pandas as pd
from datetime import date, datetime
import io

class DataTransformation:

    def __init__(self, config: dict):

        self.server_configuration = ServerConnection(config)
        self.query_configuration = self.server_configuration.query_connection()

    def extract_table(self, name, path):
        with open(path, 'r') as f:
            mustache_template = chevron.render(f, {})

        self.query_configuration.setQuery(mustache_template)
        result = self.query_configuration.query()

        if result.response.status == 200:
            print("Query succeeded!")
        else:
            sys.exit("Query failed with status code:", result.response.status)

        result = result.convert()

        if isinstance(result, list):
            result_csv = io.StringIO(result[1])
        else:
            result_csv = io.StringIO(result.decode('utf-8'))
        
        df = pd.read_csv(result_csv)
        return df
    
    
    def date_to_datetime(self,date_input):
        date = datetime.strptime(date_input, '%Y-%m-%d')
        time = datetime.min.time()
        datetime_final = datetime.combine(date, time)
        return datetime_final

    def table_person_transformation(self, df_PERSON):

        person_header = ["person_id", "gender_concept_id", "year_of_birth", "month_of_birth", "day_of_birth", "birth_datetime", "race_concept_id", "ethnicity_concept_id", "location_id", "provider_id", "care_site_id", "person_source_value", "gender_source_value", "gender_source_concept_id", "race_source_value", "race_source_concept_id", "ethnicity_source_value", "ethnicity_source_concept_id"]

        df_PERSON = df_PERSON.where(pd.notnull(df_PERSON), None)
        if 'gender_source_value' in df_PERSON.columns:
            df_PERSON.loc[df_PERSON.gender_source_value == 'http://purl.obolibrary.org/obo/NCIT_C16576', 'gender_concept_id'] = "8532"
            df_PERSON.loc[df_PERSON.gender_source_value == 'http://purl.obolibrary.org/obo/NCIT_C20197', 'gender_concept_id'] = "8507"
            df_PERSON.loc[df_PERSON.gender_source_value == 'http://purl.obolibrary.org/obo/NCIT_C124294', 'gender_concept_id'] = "9999"
            df_PERSON.loc[df_PERSON.gender_source_value == 'http://purl.obolibrary.org/obo/NCIT_C17998', 'gender_concept_id'] = "9999"
            
            
        if 'birth_datetime' in df_PERSON.columns:
            for index, row in df_PERSON.iterrows():
                # Create all date-related columns:
                date_string = df_PERSON["birth_datetime"][index]
                date = datetime.strptime(date_string, '%Y-%m-%d')  # Changed variable name here
                time = datetime.min.time()
                datetime_combined = datetime.combine(date, time)
                df_PERSON.loc[index, "birth_datetime"] = datetime_combined
                df_PERSON.loc[index, "year_of_birth"] = datetime_combined.year
                df_PERSON.loc[index, "month_of_birth"] = datetime_combined.month
                df_PERSON.loc[index, "day_of_birth"] = datetime_combined.day

            if row["race_concept_id"] is None:
                df_PERSON.loc[index, "race_concept_id"] = 0

            if row["ethnicity_concept_id"] is None:
                df_PERSON.loc[index, "ethnicity_concept_id"] = 0

        return df_PERSON


    def table_death_transformation(self,df_DEATH):

        for index, row in df_DEATH.iterrows():

            date_string = df_DEATH["death_date"][index]
            date_calculated = self.date_to_datetime(date_string)
            df_DEATH.at[index, 'death_datetime'] = date_calculated

        selected_cols = [col for col in df_DEATH.columns if col in death_header]
        df_DEATH = pd.DataFrame(df_DEATH, columns=selected_cols)
        return df_DEATH


    def table_condition_visit_transformation(self,df_CONDITION_VISIT):

        df_CONDITION_VISIT = df_CONDITION_VISIT.where(pd.notnull(df_CONDITION_VISIT), None)

        for index, row in df_CONDITION_VISIT.iterrows():
            if row["condition_type_concept_id"] == None:
                df_CONDITION_VISIT.at[index, 'condition_type_concept_id'] = 32879

            if row["condition_status_concept_id"] == None:
                df_CONDITION_VISIT.at[index, 'condition_status_concept_id'] = 32893

            if row["visit_type_concept_id"] == None:
                df_CONDITION_VISIT.at[index, 'visit_type_concept_id'] = 32879

            if row["visit_concept_id"] == None:
                df_CONDITION_VISIT.at[index, 'visit_concept_id'] = 38004515

            date_string = df_CONDITION_VISIT["condition_start_date"][index]
            date_calculated = self.date_to_datetime(date_string)
            df_CONDITION_VISIT.at[index, 'condition_start_datetime'] = date_calculated

            date_string = df_CONDITION_VISIT["condition_end_date"][index]
            date_calculated = self.date_to_datetime(date_string)
            df_CONDITION_VISIT.at[index, 'condition_end_datetime'] = date_calculated

            date_string = df_CONDITION_VISIT["visit_start_date"][index]
            date_calculated = self.date_to_datetime(date_string)
            df_CONDITION_VISIT.at[index, 'visit_start_datetime'] = date_calculated

            date_string = df_CONDITION_VISIT["visit_end_time"][index]
            date_calculated = self.date_to_datetime(date_string)
            df_CONDITION_VISIT.at[index, 'visit_end_datetime'] = date_calculated

        return df_CONDITION_VISIT
    
    def table_measurement_visit_transformation(df_MEASUREMENT_VISIT):

        df_MEASUREMENT_VISIT = df_MEASUREMENT_VISIT.where(pd.notnull(df_MEASUREMENT_VISIT), None)

        for index, row in df_MEASUREMENT_VISIT.iterrows():

            if row["visit_type_concept_id"] == None:
                df_MEASUREMENT_VISIT.at[index, 'visit_type_concept_id'] = 32879

            if row["visit_concept_id"] == None:
                df_MEASUREMENT_VISIT.at[index, 'visit_concept_id'] = 38004515

            date_string = df_MEASUREMENT_VISIT["measurement_date"][index]
            date_calculated = self.date_to_datetime(date_string)
            df_MEASUREMENT_VISIT.at[index, 'measurement_datetime'] = date_calculated

            date_string = df_MEASUREMENT_VISIT["visit_start_date"][index]
            date_calculated = self.date_to_datetime(date_string)
            df_MEASUREMENT_VISIT.at[index, 'visit_start_datetime'] = date_calculated

            date_string = df_MEASUREMENT_VISIT["visit_end_time"][index]
            date_calculated = self.date_to_datetime(date_string)
            df_MEASUREMENT_VISIT.at[index, 'visit_end_datetime'] = date_calculated












