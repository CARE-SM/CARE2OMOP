import os
import sys
import pandas as pd
from SPARQLWrapper import SPARQLWrapper, POST, BASIC, CSV
from io import StringIO


# ------------------------------------------------------------
# ServerConnection
# ------------------------------------------------------------
class ServerConnection:
    """
    Handles connection configuration for a SPARQL endpoint,
    including authentication credentials and connection setup.
    """

    def __init__(self, config: dict):
        """
        Initializes connection using parameters from a configuration dictionary.
        """
        self.triplestore_url = config.get("TRIPLESTORE_URL")
        if not self.triplestore_url:
            raise ValueError(
                "No endpoint defined in configuration. Please provide TRIPLESTORE_URL."
            )

        self.triplestore_user = config.get("TRIPLESTORE_USERNAME")
        self.triplestore_pass = config.get("TRIPLESTORE_PASSWORD")

    def query_connection(self) -> SPARQLWrapper:
        """
        Returns a configured SPARQLWrapper endpoint with BASIC authentication
        and CSV output format.
        """
        endpoint = SPARQLWrapper(self.triplestore_url)
        endpoint.setHTTPAuth(BASIC)

        # Set credentials if provided
        if self.triplestore_user and self.triplestore_pass:
            endpoint.setCredentials(self.triplestore_user, self.triplestore_pass)

        # POST method and CSV results
        endpoint.setMethod(POST)
        endpoint.setReturnFormat(CSV)
        return endpoint


# ------------------------------------------------------------
# Workflow
# ------------------------------------------------------------
class Workflow:
    """
    Handles table extraction from a SPARQL triplestore and performs
    transformations into OMOP-compatible tables.
    """

    def __init__(self, config: dict, format_dir: str):
        """
        Initializes workflow with connection settings and query directory.

        Args:
            config: Dictionary with connection parameters for the triplestore.
            format_dir: Path to directory containing SPARQL query files.
        """
        self.connection = ServerConnection(config)
        self.endpoint = self.connection.query_connection()
        self.format_dir = format_dir

    # -----------------------------
    # Helper methods
    # -----------------------------
    @staticmethod
    def fillna_defaults(df, defaults: dict):
        """
        Fills missing values in specified columns with given defaults.
        Keeps integer types consistent (avoiding unwanted float conversion).
        """
        for col, val in defaults.items():
            if col in df.columns:
                df[col] = df[col].fillna(val)

                # Try to keep integer-looking values as Int64 (nullable int)
                if pd.api.types.is_float_dtype(df[col]) and all(
                    str(v).replace('.', '', 1).isdigit() or pd.isna(v) for v in df[col]
                ):
                    try:
                        df[col] = df[col].astype("Int64")
                    except Exception:
                        pass
        return df

    @staticmethod
    def convert_dates(df, date_cols: list):
        """Converts specified columns to datetime objects."""
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")
        return df

    @staticmethod
    def map_values(df, source_col: str, mapping: dict, target_col: str = None):
        """Maps values in a column according to a dictionary mapping."""
        if source_col in df.columns:
            df[target_col or source_col] = df[source_col].map(mapping).fillna(
                df.get(target_col, df[source_col])
            )
        return df

    # -----------------------------
    # Table extraction
    # -----------------------------
    def extract_table(self, name: str) -> pd.DataFrame:
        """
        Executes all SPARQL queries in the format directory that start with the given name.
        Queries are executed against the authenticated triplestore.

        Args:
            name: Table name prefix to match SPARQL files.

        Returns:
            Combined DataFrame with query results.
        """
        format_files = os.listdir(self.format_dir)
        dfs = []

        for file in format_files:
            if file.startswith(name):
                query_path = os.path.join(self.format_dir, file)
                with open(query_path, "r") as q:
                    query = q.read()

                # Reuse the existing authenticated SPARQLWrapper endpoint
                sparql = self.endpoint
                sparql.setQuery(query)

                try:
                    result = sparql.query()
                except Exception as e:
                    sys.exit(f"SPARQL query failed: {e}")

                # Parse CSV response into a DataFrame
                response = result.response.read().decode("utf-8")
                df = pd.read_csv(StringIO(response))
                dfs.append(df)

        # Return concatenated DataFrame or empty one if no data
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


    # -----------------------------
    # Table-specific transformations
    # -----------------------------
    def table_person_transformation(self, df):
        """
        Cleans and enriches a PERSON table according to OMOP conventions.
        """
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

        # Gender mapping: URIs to OMOP concept IDs
        gender_map = {
            "http://purl.obolibrary.org/obo/NCIT_C20197": 8507,  # Male
            "http://purl.obolibrary.org/obo/NCIT_C16576": 8532,  # Female
        }
        df = self.map_values(df, "gender_source_value", gender_map, "gender_concept_id")

        # Convert and extract birth date components
        df = self.convert_dates(df, ["birth_datetime"])
        if "birth_datetime" in df.columns:
            df["year_of_birth"] = df["birth_datetime"].dt.year
            df["month_of_birth"] = df["birth_datetime"].dt.month
            df["day_of_birth"] = df["birth_datetime"].dt.day

        return df

    def table_death_transformation(self, df):
        """Cleans and enriches a DEATH table."""
        df = df.copy()
        df = self.convert_dates(df, ["death_date"])
        if "death_date" in df.columns:
            df["death_datetime"] = df["death_date"]
        return df

    def table_condition_transformation(self, df):
        """Transforms CONDITION_OCCURRENCE table to match OMOP model."""
        df = df.copy()
        df = self.fillna_defaults(df, {"condition_type_concept_id": 32879})
        df = self.convert_dates(
            df,
            [
                "condition_start_date",
                "condition_end_date",
                "visit_start_date",
                "visit_end_date",
            ],
        )
        if "condition_start_date" in df.columns:
            df["condition_start_datetime"] = df["condition_start_date"]
        if "condition_end_date" in df.columns:
            df["condition_end_datetime"] = df["condition_end_date"]
        if "visit_start_date" in df.columns:
            df["visit_start_datetime"] = df["visit_start_date"]
        if "visit_end_date" in df.columns:
            df["visit_end_datetime"] = df["visit_end_date"]
        return df

    def table_measurement_transformation(self, df):
        """Transforms MEASUREMENT table to match OMOP model."""
        df = df.copy()
        df = self.fillna_defaults(df, {"measurement_type_concept_id": 32879})
        df = self.convert_dates(df, ["measurement_date", "visit_start_date", "visit_end_date"])
        if "measurement_date" in df.columns:
            df["measurement_datetime"] = df["measurement_date"]
        if "visit_start_date" in df.columns:
            df["visit_start_datetime"] = df["visit_start_date"]
        if "visit_end_date" in df.columns:
            df["visit_end_datetime"] = df["visit_end_date"]

        # Example: Map measurement source URIs to OMOP concept IDs
        if "measurement_source_concept_id" in df.columns:
            df.loc[
                df.measurement_source_concept_id == "http://purl.obolibrary.org/obo/NCIT_C16358",
                "measurement_concept_id",
            ] = 4245997
            df.loc[
                df.measurement_source_concept_id == "http://purl.obolibrary.org/obo/NCIT_C25347",
                "measurement_concept_id",
            ] = 903133
            df.loc[
                df.measurement_source_concept_id == "http://purl.obolibrary.org/obo/NCIT_C25208",
                "measurement_concept_id",
            ] = 903121

        return df

    def table_observation_transformation(self, df):
        """Transforms OBSERVATION table to match OMOP model."""
        df = df.copy()
        df = self.fillna_defaults(df, {"observation_type_concept_id": 32879})
        df = self.convert_dates(df, ["observation_date", "visit_start_date", "visit_end_date"])
        if "observation_date" in df.columns:
            df["observation_datetime"] = df["observation_date"]
        if "visit_start_date" in df.columns:
            df["visit_start_datetime"] = df["visit_start_date"]
        if "visit_end_date" in df.columns:
            df["visit_end_datetime"] = df["visit_end_date"]
        return df
    
    def table_observation_period_transformation(self, df):
        """Transforms OBSERVATION_PERIOD table to match OMOP model."""
        df = df.copy()
        df = self.fillna_defaults(df, {"period_type_concept_id": 32879})
        return df
    
    def table_visit_occurrence_transformation(self, df):
        """Transforms VISIT_OCCURRENCE table to match OMOP model."""
        df = df.copy()
        df = self.fillna_defaults(df, {"visit_type_concept_id": 32879})
        df = self.fillna_defaults(df, {"visit_concept_id": 38004515})

        df = self.convert_dates(df, ["visit_start_date", "visit_end_date"])
        if "visit_start_date" in df.columns:
            df["visit_start_datetime"] = df["visit_start_date"]
        if "visit_end_date" in df.columns:
            df["visit_end_datetime"] = df["visit_end_date"]
        return df


    def table_drug_exposure_transformation(self, df):
        """Transforms DRUG_EXPOSURE table to match OMOP model."""
        df = df.copy()
        df = self.fillna_defaults(df, {"drug_type_concept_id": 32879})
        df = self.convert_dates(
            df,
            [
                "drug_exposure_start_date",
                "drug_exposure_end_date",
                "visit_start_date",
                "visit_end_date",
            ],
        )
        if "drug_exposure_start_date" in df.columns:
            df["drug_exposure_start_datetime"] = df["drug_exposure_start_date"]
        if "drug_exposure_end_date" in df.columns:
            df["drug_exposure_end_datetime"] = df["drug_exposure_end_date"]
        if "visit_start_date" in df.columns:
            df["visit_start_datetime"] = df["visit_start_date"]
        if "visit_end_date" in df.columns:
            df["visit_end_datetime"] = df["visit_end_date"]
        return df

    def table_procedure_occurrence_transformation(self, df):
        """Transforms PROCEDURE_OCCURRENCE table to match OMOP model."""
        df = df.copy()
        df = self.fillna_defaults(df, {"procedure_type_concept_id": 32879})
        df = self.convert_dates(
            df,
            ["procedure_date", "procedure_end_date", "visit_start_date", "visit_end_date"],
        )
        if "procedure_date" in df.columns:
            df["procedure_datetime"] = df["procedure_date"]
        if "procedure_end_date" in df.columns:
            df["procedure_end_datetime"] = df["procedure_end_date"]
        if "visit_start_date" in df.columns:
            df["visit_start_datetime"] = df["visit_start_date"]
        if "visit_end_date" in df.columns:
            df["visit_end_datetime"] = df["visit_end_date"]
        return df
