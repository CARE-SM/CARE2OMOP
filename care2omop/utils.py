import chevron
from auth import ServerConnection
import pandas as pd
from datetime import date, datetime
import io
import sys
from os import listdir
from os.path import isfile, join

class DataTransformation:

    def __init__(self, config: dict):

        self.server_configuration = ServerConnection(config)
        self.query_configuration = self.server_configuration.query_connection()

    def extract_table(self, name):
        
        df_final = pd.DataFrame()
        path = "templates/"
        format_file = "mustache"
        
        ## Search the templates

        files = [f for f in listdir(path) if isfile(join(path, f))]
        format_files = [ff for ff in files if ff.endswith(str("." + format_file))]
        if len(format_files) == 0:
            sys.exit("No resources are present at {} path with .{} format.".format(path,format))
 
        ## For each template, run the query:
        
        for file in format_files:

            if file.startswith(name):
                
                full_path = path + file
                
                with open(full_path, 'r') as f:
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
                    
                df_result = pd.read_csv(result_csv)
                df_final = pd.concat([df_final, df_result])
                df_final = df_final.reset_index(drop=True)


        return df_final
    
    
    def date_to_datetime(self,date_input):
        if date_input:
            date = datetime.strptime(date_input, '%Y-%m-%d')
            time = datetime.min.time()
            datetime_final = datetime.combine(date, time)
            return datetime_final

    def table_person_transformation(self, df_PERSON):

        df_PERSON = df_PERSON.where(pd.notnull(df_PERSON), None)
        for index, row in df_PERSON.iterrows():

            if row["race_concept_id"] is None:
                df_PERSON.loc[index, "race_concept_id"] = 0

            if row["ethnicity_concept_id"] is None:
                df_PERSON.loc[index, "ethnicity_concept_id"] = 0

            if 'gender_source_value' in df_PERSON.columns:
                df_PERSON.loc[df_PERSON.gender_source_value == 'http://purl.obolibrary.org/obo/NCIT_C16576', 'gender_concept_id'] = "8532"
                df_PERSON.loc[df_PERSON.gender_source_value == 'http://purl.obolibrary.org/obo/NCIT_C20197', 'gender_concept_id'] = "8507"
                df_PERSON.loc[df_PERSON.gender_source_value == 'http://purl.obolibrary.org/obo/NCIT_C124294', 'gender_concept_id'] = "9999"
                df_PERSON.loc[df_PERSON.gender_source_value == 'http://purl.obolibrary.org/obo/NCIT_C17998', 'gender_concept_id'] = "9999"  
                
            if 'birth_datetime' in df_PERSON.columns:
                date_string = df_PERSON["birth_datetime"][index]
                date = datetime.strptime(date_string, '%Y-%m-%d')
                time = datetime.min.time()
                datetime_combined = datetime.combine(date, time)
                df_PERSON.loc[index, "birth_datetime"] = datetime_combined
                df_PERSON.loc[index, "year_of_birth"] = datetime_combined.year
                df_PERSON.loc[index, "month_of_birth"] = datetime_combined.month
                df_PERSON.loc[index, "day_of_birth"] = datetime_combined.day
                
        return df_PERSON


    def table_death_transformation(self,df_DEATH):
        
        df_DEATH = df_DEATH.where(pd.notnull(df_DEATH), None)
        for index, row in df_DEATH.iterrows():

            if row["death_type_concept_id"] == None:
                df_DEATH.at[index, 'death_type_concept_id'] = 32879

            date_string = df_DEATH["death_date"][index]
            date_calculated = self.date_to_datetime(date_string)
            df_DEATH.at[index, 'death_datetime'] = date_calculated
            
        return df_DEATH

    def table_condition_transformation(self,df_CONDITION):

        df_CONDITION = df_CONDITION.where(pd.notnull(df_CONDITION), None)

        for index, row in df_CONDITION.iterrows():
            if row["condition_type_concept_id"] == None:
                df_CONDITION.at[index, 'condition_type_concept_id'] = 32879

            if row["condition_status_concept_id"] == None:
                df_CONDITION.at[index, 'condition_status_concept_id'] = 32893

            if row["visit_type_concept_id"] == None:
                df_CONDITION.at[index, 'visit_type_concept_id'] = 32879

            if row["visit_concept_id"] == None:
                df_CONDITION.at[index, 'visit_concept_id'] = 38004515
                
            if row["condition_concept_id"] is None:
                df_CONDITION.loc[index, "condition_concept_id"] = 0

            date_string = df_CONDITION["condition_start_date"][index]
            date_calculated = self.date_to_datetime(date_string)
            df_CONDITION.at[index, 'condition_start_datetime'] = date_calculated

            date_string = df_CONDITION["condition_end_date"][index]
            date_calculated = self.date_to_datetime(date_string)
            df_CONDITION.at[index, 'condition_end_datetime'] = date_calculated

            date_string = df_CONDITION["visit_start_date"][index]
            date_calculated = self.date_to_datetime(date_string)
            df_CONDITION.at[index, 'visit_start_datetime'] = date_calculated

            date_string = df_CONDITION["visit_end_time"][index]
            date_calculated = self.date_to_datetime(date_string)
            df_CONDITION.at[index, 'visit_end_datetime'] = date_calculated

        return df_CONDITION
    
    def table_measurement_transformation(self,df_MEASUREMENT):

        df_MEASUREMENT = df_MEASUREMENT.where(pd.notnull(df_MEASUREMENT), None)

        for index, row in df_MEASUREMENT.iterrows():

            if row["visit_type_concept_id"] == None:
                df_MEASUREMENT.at[index, 'visit_type_concept_id'] = 32879
                
            if row["measurement_type_concept_id"] == None:
                df_MEASUREMENT.at[index, 'measurement_type_concept_id'] = 32879

            if row["visit_concept_id"] == None:
                df_MEASUREMENT.at[index, 'visit_concept_id'] = 38004515
                
            if row["measurement_concept_id"] is None:
                df_MEASUREMENT.loc[index, "measurement_concept_id"] = 0

            date_string = df_MEASUREMENT["measurement_date"][index]
            date_calculated = self.date_to_datetime(date_string)
            df_MEASUREMENT.at[index, 'measurement_datetime'] = date_calculated

            date_string = df_MEASUREMENT["visit_start_date"][index]
            date_calculated = self.date_to_datetime(date_string)
            df_MEASUREMENT.at[index, 'visit_start_datetime'] = date_calculated

            date_string = df_MEASUREMENT["visit_end_time"][index]
            date_calculated = self.date_to_datetime(date_string)
            df_MEASUREMENT.at[index, 'visit_end_datetime'] = date_calculated            
            
            if 'measurement_concept_id' in df_MEASUREMENT.columns:
                df_MEASUREMENT.loc[df_MEASUREMENT.measurement_source_concept_id == 'http://purl.obolibrary.org/obo/NCIT_C16358', 'measurement_concept_id'] = 4245997
                df_MEASUREMENT.loc[df_MEASUREMENT.measurement_source_concept_id == 'http://purl.obolibrary.org/obo/NCIT_C25347', 'measurement_concept_id'] = 903133
                df_MEASUREMENT.loc[df_MEASUREMENT.measurement_source_concept_id == 'http://purl.obolibrary.org/obo/NCIT_C25208', 'measurement_concept_id'] = 903121
            
        return df_MEASUREMENT 
            
    def table_observation_transformation(self,df_OBSERVATION):

        df_OBSERVATION = df_OBSERVATION.where(pd.notnull(df_OBSERVATION), None)

        for index, row in df_OBSERVATION.iterrows():
            
            if row["visit_type_concept_id"] == None:
                df_OBSERVATION.at[index, 'visit_type_concept_id'] = 32879

            if row["visit_concept_id"] == None:
                df_OBSERVATION.at[index, 'visit_concept_id'] = 38004515
                
            if row["observation_type_concept_id"] == None:
                df_OBSERVATION.at[index, 'observation_type_concept_id'] = 32879
    
            if row["observation_concept_id"] is None:
                df_OBSERVATION.loc[index, "observation_concept_id"] = 0
                           
            date_string = df_OBSERVATION["observation_date"][index]
            date_calculated = self.date_to_datetime(date_string)
            df_OBSERVATION.at[index, 'observation_datetime'] = date_calculated                

            date_string = df_OBSERVATION["visit_start_date"][index]
            date_calculated = self.date_to_datetime(date_string)
            df_OBSERVATION.at[index, 'visit_start_datetime'] = date_calculated

            date_string = df_OBSERVATION["visit_end_time"][index]
            date_calculated = self.date_to_datetime(date_string)
            df_OBSERVATION.at[index, 'visit_end_datetime'] = date_calculated

        return df_OBSERVATION 

    def table_procedure_transformation(self,df_PROCEDURE):

        df_PROCEDURE = df_PROCEDURE.where(pd.notnull(df_PROCEDURE), None)

        for index, row in df_PROCEDURE.iterrows():
            
            if row["visit_type_concept_id"] == None:
                df_PROCEDURE.at[index, 'visit_type_concept_id'] = 32879

            if row["visit_concept_id"] == None:
                df_PROCEDURE.at[index, 'visit_concept_id'] = 38004515
                
            if row["procedure_type_concept_id"] == None:
                df_PROCEDURE.at[index, 'procedure_type_concept_id'] = 32879   
                
            if row["procedure_concept_id"] is None:
                df_PROCEDURE.loc[index, "procedure_concept_id"] = 0
                           
            date_string = df_PROCEDURE["procedure_date"][index]
            date_calculated = self.date_to_datetime(date_string)
            df_PROCEDURE.at[index, 'procedure_datetime'] = date_calculated    
            
            date_string = df_PROCEDURE["procedure_end_date"][index]
            date_calculated = self.date_to_datetime(date_string)
            df_PROCEDURE.at[index, 'procedure_end_datetime'] = date_calculated               

            date_string = df_PROCEDURE["visit_start_date"][index]
            date_calculated = self.date_to_datetime(date_string)
            df_PROCEDURE.at[index, 'visit_start_datetime'] = date_calculated

            date_string = df_PROCEDURE["visit_end_time"][index]
            date_calculated = self.date_to_datetime(date_string)
            df_PROCEDURE.at[index, 'visit_end_datetime'] = date_calculated
         
        return df_PROCEDURE 

    def table_drug_transformation(self,df_DRUG_EXPOSE):

        df_DRUG_EXPOSE = df_DRUG_EXPOSE.where(pd.notnull(df_DRUG_EXPOSE), None)

        for index, row in df_DRUG_EXPOSE.iterrows():
            
            if row["visit_type_concept_id"] == None:
                df_DRUG_EXPOSE.at[index, 'visit_type_concept_id'] = 32879

            if row["visit_concept_id"] == None:
                df_DRUG_EXPOSE.at[index, 'visit_concept_id'] = 38004515
                
            if row["drug_type_concept_id"] == None:
                df_DRUG_EXPOSE.at[index, 'drug_type_concept_id'] = 32879
                               
            date_string = df_DRUG_EXPOSE["drug_exposure_start_date"][index]
            date_calculated = self.date_to_datetime(date_string)
            df_DRUG_EXPOSE.at[index, 'drug_exposure_start_datetime'] = date_calculated    
            
            date_string = df_DRUG_EXPOSE["drug_exposure_end_date"][index]
            date_calculated = self.date_to_datetime(date_string)
            df_DRUG_EXPOSE.at[index, 'drug_exposure_end_datetime'] = date_calculated               

            date_string = df_DRUG_EXPOSE["visit_start_date"][index]
            date_calculated = self.date_to_datetime(date_string)
            df_DRUG_EXPOSE.at[index, 'visit_start_datetime'] = date_calculated

            date_string = df_DRUG_EXPOSE["visit_end_time"][index]
            date_calculated = self.date_to_datetime(date_string)
            df_DRUG_EXPOSE.at[index, 'visit_end_datetime'] = date_calculated

        return df_DRUG_EXPOSE 