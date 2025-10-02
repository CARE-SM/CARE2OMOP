import os
import sys
import pandas as pd
from SPARQLWrapper import SPARQLWrapper, CSV
from io import StringIO


class Workflow:
    def __init__(self, endpoint, format_dir):
        self.endpoint = endpoint
        self.format_dir = format_dir

    # -----------------------------
    # Helpers
    # -----------------------------
    @staticmethod
    def fillna_defaults(df, defaults: dict):
        """Rellena valores nulos en columnas con valores por defecto."""
        for col, val in defaults.items():
            if col in df.columns:
                df[col] = df[col].fillna(val)
        return df

    @staticmethod
    def convert_dates(df, date_cols: list):
        """Convierte columnas de fecha a datetime."""
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")
        return df

    @staticmethod
    def map_values(df, source_col: str, mapping: dict, target_col: str = None):
        """Mapea valores de una columna a otra según un diccionario."""
        if source_col in df.columns:
            df[target_col or source_col] = df[source_col].map(mapping).fillna(
                df.get(target_col, df[source_col])
            )
        return df

    # -----------------------------
    # Extracción de tablas
    # -----------------------------
    def extract_table(self, name):
        format_files = os.listdir(self.format_dir)
        dfs = []
        print(dfs)

        for file in format_files:
            if file.startswith(name):
                with open(os.path.join(self.format_dir, file), "r") as q:
                    query = q.read()
                    sparql = SPARQLWrapper(self.endpoint)
                    sparql.setQuery(query)
                    sparql.setReturnFormat(CSV)
                    result = sparql.query()

                    if result.response.status != 200:
                        sys.exit(
                            f"Query failed with status code: {result.response.status}"
                        )

                    df = pd.read_csv(StringIO(result.response.read().decode("utf-8")))
                    dfs.append(df)

        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    # -----------------------------
    # Transformaciones específicas
    # -----------------------------
    def table_person_transformation(self, df):
        df = df.copy()
        df = self.fillna_defaults(
            df,
            {
                "race_concept_id": 0,
                "ethnicity_concept_id": 0,
                "race_source_value": "NA",
                "ethnicity_source_value": "NA",
            },
        )

        # Mapear género
        gender_map = {
            "http://purl.obolibrary.org/obo/NCIT_C20197": 8507,  # Male
            "http://purl.obolibrary.org/obo/NCIT_C16576": 8532,  # Female
        }
        df = self.map_values(df, "gender_source_value", gender_map, "gender_concept_id")

        # Fechas de nacimiento
        df = self.convert_dates(df, ["birth_datetime"])
        if "birth_datetime" in df.columns:
            df["year_of_birth"] = df["birth_datetime"].dt.year
            df["month_of_birth"] = df["birth_datetime"].dt.month
            df["day_of_birth"] = df["birth_datetime"].dt.day

        return df

    def table_death_transformation(self, df):
        df = df.copy()
        df = self.convert_dates(df, ["death_date"])
        if "death_date" in df.columns:
            df["death_datetime"] = df["death_date"]
        return df

    def table_condition_transformation(self, df):
        df = df.copy()
        df = self.fillna_defaults(df, {"condition_type_concept_id": 32879})
        df = self.convert_dates(df, ["condition_start_date", "condition_end_date", "visit_start_date", "visit_end_time"]) 
        if "condition_start_date" in df.columns:
            df["condition_start_datetime"] = df["condition_start_date"]
        if "condition_end_date" in df.columns:
            df["condition_end_datetime"] = df["condition_end_date"]
        # Normalizar visit start/end
        if "visit_start_date" in df.columns:
            df["visit_start_datetime"] = df["visit_start_date"]
        if "visit_end_time" in df.columns:
            df["visit_end_datetime"] = df["visit_end_date"]
        return df

    def table_measurement_transformation(self, df):
        df = df.copy()
        df = self.fillna_defaults(df, {"measurement_type_concept_id": 32879})
        df = self.convert_dates(df, ["measurement_date", "visit_start_date", "visit_end_time"]) 
        if "measurement_date" in df.columns:
            df["measurement_datetime"] = df["measurement_date"]
        if "visit_start_date" in df.columns:
            df["visit_start_datetime"] = df["visit_start_date"]
        if "visit_end_time" in df.columns:
            df["visit_end_datetime"] = df["visit_end_date"]

        # Ejemplo de mapeo de measurement_source_concept_id (si es URI -> map a concepto OMOP)
        if "measurement_source_concept_id" in df.columns:
            df.loc[df.measurement_source_concept_id == 'http://purl.obolibrary.org/obo/NCIT_C16358', 'measurement_concept_id'] = 4245997
            df.loc[df.measurement_source_concept_id == 'http://purl.obolibrary.org/obo/NCIT_C25347', 'measurement_concept_id'] = 903133
            df.loc[df.measurement_source_concept_id == 'http://purl.obolibrary.org/obo/NCIT_C25208', 'measurement_concept_id'] = 903121

        return df

    def table_observation_transformation(self, df):
        df = df.copy()
        df = self.fillna_defaults(df, {"observation_type_concept_id": 32879})
        df = self.convert_dates(df, ["observation_date", "visit_start_date", "visit_end_time"]) 
        if "observation_date" in df.columns:
            df["observation_datetime"] = df["observation_date"]
        if "visit_start_date" in df.columns:
            df["visit_start_datetime"] = df["visit_start_date"]
        if "visit_end_time" in df.columns:
            df["visit_end_datetime"] = df["visit_end_date"]
        return df

    def table_drug_exposure_transformation(self, df):
        df = df.copy()
        df = self.fillna_defaults(df, {"drug_type_concept_id": 32879})
        df = self.convert_dates(df, ["drug_exposure_start_date", "drug_exposure_end_date", "visit_start_date", "visit_end_time"]) 
        if "drug_exposure_start_date" in df.columns:
            df["drug_exposure_start_datetime"] = df["drug_exposure_start_date"]
        if "drug_exposure_end_date" in df.columns:
            df["drug_exposure_end_datetime"] = df["drug_exposure_end_date"]
        if "visit_start_date" in df.columns:
            df["visit_start_datetime"] = df["visit_start_date"]
        if "visit_end_time" in df.columns:
            df["visit_end_datetime"] = df["visit_end_date"]
        return df

    def table_procedure_occurrence_transformation(self, df):
        df = df.copy()
        df = self.fillna_defaults(df, {"procedure_type_concept_id": 32879})
        df = self.convert_dates(df, ["procedure_date", "procedure_end_date", "visit_start_date", "visit_end_time"]) 
        if "procedure_date" in df.columns:
            df["procedure_datetime"] = df["procedure_date"]
        if "procedure_end_date" in df.columns:
            df["procedure_end_datetime"] = df["procedure_end_date"]
        if "visit_start_date" in df.columns:
            df["visit_start_datetime"] = df["visit_start_date"]
        if "visit_end_time" in df.columns:
            df["visit_end_datetime"] = df["visit_end_date"]
        return df