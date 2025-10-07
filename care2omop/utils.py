import os
import sys
import pandas as pd
from SPARQLWrapper import SPARQLWrapper, POST, BASIC, CSV
from io import StringIO
import re

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

    def __init__(self, endpoint: str, format_dir: str, mapping_csv_path: str | None = None):
        """
        Initialize the Workflow, loading SNOMED→ATHENA mapping if provided.

        Args:
            endpoint (str): Your data endpoint or connection URL.
            format_dir (str): Directory path for format templates or outputs.
            mapping_csv_path (str, optional): Path to SNOMED→ATHENA mapping CSV.
        """
        self.endpoint = endpoint
        self.format_dir = format_dir
        self.snomed_mapping = None

        if mapping_csv_path:
            self.snomed_mapping = self._load_snomed_mapping(mapping_csv_path)

    # -----------------------------
    # Helper methods
    # -----------------------------
    @staticmethod
    def _load_snomed_mapping(mapping_csv_path: str) -> dict:
        """Load SNOMED→ATHENA mapping into memory as a dictionary."""
        mapping_df = pd.read_csv(mapping_csv_path, dtype=str, usecols=["concept_code", "concept_id"])
        mapping_df.dropna(subset=["concept_code", "concept_id"], inplace=True)
        mapping = dict(zip(mapping_df["concept_code"], mapping_df["concept_id"]))
        return mapping

    @staticmethod
    def _strip_snomed_prefix(value: str) -> str | None:
        """
        If the value is a SNOMED URI, remove the prefix and return the ID.
        If it's another URI, return None to skip mapping.
        """
        if not isinstance(value, str):
            return None

        value = value.strip()
        if value.startswith("http://snomed.info/id/"):
            return value.replace("http://snomed.info/id/", "").strip()
        elif re.match(r"^https?://", value):
            return None
        else:
            return value
        
    @staticmethod
    def fillna_defaults(df, defaults: dict):
        """Fills missing values in specified columns with given defaults,
        and ensures integer-like columns retain Int64 dtype."""
        for col, val in defaults.items():
            if col in df.columns:
                df[col] = df[col].fillna(val)
                # If column looks numeric, try to convert to Int64
                if pd.api.types.is_float_dtype(df[col]) or pd.api.types.is_object_dtype(df[col]):
                    try:
                        # Convert numeric strings or floats that represent integers
                        df[col] = pd.to_numeric(df[col], errors="ignore")
                        if pd.api.types.is_numeric_dtype(df[col]):
                            # If all values are integers or NaN, use Int64
                            if (df[col].dropna() % 1 == 0).all():
                                df[col] = df[col].astype("Int64")
                    except Exception:
                        pass

        # Additional global pass: fix any float columns that are really integers
        for col in df.columns:
            if pd.api.types.is_float_dtype(df[col]):
                try:
                    if (df[col].dropna() % 1 == 0).all():
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
    # SNOMED → ATHENA mapping
    # -----------------------------
    def map_snomed_to_athena(self, df: pd.DataFrame, column_pairs: list[tuple[str, str]]) -> pd.DataFrame:
        """
        Maps SNOMED codes (possibly full URLs) in one or more columns to ATHENA concept IDs.

        Args:
            df (pd.DataFrame): The dataframe to process.
            column_pairs (list[tuple[str, str]]): Each tuple defines:
                (source_column, target_column)
                - source_column: column containing SNOMED codes or URIs
                - target_column: column to receive ATHENA IDs

        Returns:
            pd.DataFrame: The dataframe with updated ATHENA concept IDs.
        """
        if self.snomed_mapping is None:
            raise ValueError(
                "SNOMED→ATHENA mapping not loaded. Provide a mapping_csv_path in Workflow init."
            )

        df = df.copy()

        for source_col, target_col in column_pairs:
            if source_col not in df.columns:
                print(f"Warning: column '{source_col}' not found, skipping.")
                continue

            if target_col not in df.columns:
                df[target_col] = None

            # Step 1 — strip SNOMED prefix where appropriate
            df[source_col] = df[source_col].apply(self._strip_snomed_prefix)

            # Step 2 — map SNOMED → ATHENA (only where code exists)
            df[target_col] = df[source_col].map(self.snomed_mapping).fillna(df[target_col])

        return df

    # -----------------------------
    # Table extraction
    # -----------------------------
    def extract_table(self, name: str) -> pd.DataFrame:
        """Executes all SPARQL queries in the format directory that start with the given name."""
        format_files = os.listdir(self.format_dir)
        dfs = []

        for file in format_files:
            if file.startswith(name):
                query_path = os.path.join(self.format_dir, file)
                with open(query_path, "r") as q:
                    query = q.read()

                # ✅ Ensure endpoint is a SPARQLWrapper
                if not hasattr(self.endpoint, "setQuery"):
                    raise TypeError(
                        f"Workflow.endpoint must be a SPARQLWrapper instance, not {type(self.endpoint)}"
                    )

                sparql = self.endpoint
                sparql.setQuery(query)

                try:
                    result = sparql.query()
                except Exception as e:
                    sys.exit(f"SPARQL query failed: {e}")

                response = result.response.read().decode("utf-8")
                df = pd.read_csv(StringIO(response))
                dfs.append(df)

        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


    # -----------------------------
    # Table-specific transformations
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

        gender_map = {
            "http://snomed.info/id/248153007": 8507,  # Male
            "http://snomed.info/id/248152002": 8532,  # Female
        }
        df = self.map_values(df, "gender_source_value", gender_map, "gender_concept_id")

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
        df = self.convert_dates(df, ["condition_start_date", "condition_end_date", "visit_start_date", "visit_end_date"])

        if "condition_start_date" in df.columns:
            df["condition_start_datetime"] = df["condition_start_date"]
        if "condition_end_date" in df.columns:
            df["condition_end_datetime"] = df["condition_end_date"]
        if "visit_start_date" in df.columns:
            df["visit_start_datetime"] = df["visit_start_date"]
        if "visit_end_date" in df.columns:
            df["visit_end_datetime"] = df["visit_end_date"]

        # SNOMED→ATHENA mapping
        df = self.map_snomed_to_athena(df, [("condition_source_value", "condition_concept_id")])

        return df

    def table_measurement_transformation(self, df):
        df = df.copy()
        df = self.fillna_defaults(df, {"measurement_type_concept_id": 32879})
        df = self.convert_dates(df, ["measurement_date", "visit_start_date", "visit_end_date"])

        if "measurement_date" in df.columns:
            df["measurement_datetime"] = df["measurement_date"]
        if "visit_start_date" in df.columns:
            df["visit_start_datetime"] = df["visit_start_date"]
        if "visit_end_date" in df.columns:
            df["visit_end_datetime"] = df["visit_end_date"]

        # SNOMED→ATHENA mapping
        df = self.map_snomed_to_athena(df, [("measurement_source_value", "measurement_concept_id")])

        return df

    def table_observation_transformation(self, df):
        df = df.copy()
        df = self.fillna_defaults(df, {"observation_type_concept_id": 32879})
        df = self.convert_dates(df, ["observation_date", "visit_start_date", "visit_end_date"])

        if "observation_date" in df.columns:
            df["observation_datetime"] = df["observation_date"]
        if "visit_start_date" in df.columns:
            df["visit_start_datetime"] = df["visit_start_date"]
        if "visit_end_date" in df.columns:
            df["visit_end_datetime"] = df["visit_end_date"]

        # SNOMED→ATHENA mapping
        df = self.map_snomed_to_athena(df, [("observation_source_value", "observation_concept_id")])

        return df

    def table_observation_period_transformation(self, df):
        df = df.copy()
        df = self.fillna_defaults(df, {"period_type_concept_id": 32879})
        return df

    def table_visit_occurrence_transformation(self, df):
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
        df = df.copy()
        df = self.fillna_defaults(df, {"drug_type_concept_id": 32879})
        df = self.convert_dates(df, ["drug_exposure_start_date", "drug_exposure_end_date", "visit_start_date", "visit_end_date"])

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
        df = df.copy()
        df = self.fillna_defaults(df, {"procedure_type_concept_id": 32879})
        df = self.convert_dates(df, ["procedure_date", "procedure_end_date", "visit_start_date", "visit_end_date"])

        if "procedure_date" in df.columns:
            df["procedure_datetime"] = df["procedure_date"]
        if "procedure_end_date" in df.columns:
            df["procedure_end_datetime"] = df["procedure_end_date"]
        if "visit_start_date" in df.columns:
            df["visit_start_datetime"] = df["visit_start_date"]
        if "visit_end_date" in df.columns:
            df["visit_end_datetime"] = df["visit_end_date"]

        return df
