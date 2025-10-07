import os
import pandas as pd
from utils import Workflow, ServerConnection
from cols import (
    person_cols,
    death_cols,
    condition_occurrence_cols,
    measurement_cols,
    observation_cols,
    observation_period_cols,
    drug_exposure_cols,
    procedure_occurrence_cols,
    visit_occurrence_cols,
)

class CARE2OMOP:
    """
    CARE2OMOP orchestrates the full ETL pipeline:
      1. Extract tables from a triplestore (SPARQL)
      2. Transform into OMOP-compatible DataFrames
      3. Export to CSVs per OMOP domain
      4. Build VISIT_OCCURRENCE from visit-level data
    """

    def __init__(self, config, format_dir):
        """
        Initialize the CARE2OMOP workflow.

        Parameters
        ----------
        config : dict
            Configuration with TRIPLESTORE_URL, USERNAME, and PASSWORD.
        format_dir : str
            Directory containing SPARQL query templates.
        """
        mapping_path = "mapping/snomed_to_athena.csv"
        server = ServerConnection(config)
        endpoint = server.query_connection()
        self.workflow = Workflow(endpoint, format_dir, mapping_path)


    # -------------------------------------------------------------------------
    # Core table processor
    # -------------------------------------------------------------------------
    def process_table(self, name, transform_func, cols, out_name=None):
        """
        Extracts, transforms, and saves an OMOP table.
        Returns the *full* DataFrame (including additional columns like visit_*),
        but only writes the selected OMOP columns to CSV.

        Parameters
        ----------
        name : str
            Base name of the SPARQL query template(s) to execute
        transform_func : function
            Transformation function from Workflow to clean and map the data
        cols : list
            List of OMOP columns to retain in the final CSV file
        out_name : str, optional
            Custom name for the output CSV (defaults to `name`)

        Returns
        -------
        pd.DataFrame
            Full transformed DataFrame (including visit_* columns)
        """
        # Extract and transform the table
        df = self.workflow.extract_table(name)
        df = transform_func(df)

        # Prepare CSV output with only the OMOP columns
        if cols:
            selected = [c for c in df.columns if c in cols]
            df_out = df[selected].copy() if selected else pd.DataFrame(columns=cols)
        else:
            df_out = df.copy()

        # Write the filtered subset to CSV
        out_name = out_name or name
        os.makedirs("data", exist_ok=True)
        out_path = f"data/{out_name}.csv"
        df_out.to_csv(out_path, index=False)
        print(f"{out_name} table created → {out_path}")

        # Return the full DataFrame (for VISIT_OCCURRENCE construction)
        return df



    # -------------------------------------------------------------------------
    # Full ETL runner
    # -------------------------------------------------------------------------
    def run(self):
        """
        Runs the full ETL pipeline:
        1. Processes each OMOP domain table (PERSON, CONDITION, etc.)
        2. Constructs the VISIT_OCCURRENCE table by combining visit-related data
        """

        # -----------------------------
        # Step 1: Process core OMOP tables
        # -----------------------------
        df_person = self.process_table(
            "PERSON",
            self.workflow.table_person_transformation,
            person_cols,
            out_name="PERSON",
        )
        df_death = self.process_table(
            "DEATH",
            self.workflow.table_death_transformation,
            death_cols,
            out_name="DEATH",
        )
        df_condition = self.process_table(
            "CONDITION",
            self.workflow.table_condition_transformation,
            condition_occurrence_cols,
            out_name="CONDITION",
        )
        df_measurement = self.process_table(
            "MEASUREMENT",
            self.workflow.table_measurement_transformation,
            measurement_cols,
            out_name="MEASUREMENT",
        )
        df_observation = self.process_table(
            "OBSERVATION",
            self.workflow.table_observation_transformation,
            observation_cols,
            out_name="OBSERVATION",
        )
        df_observation_period = self.process_table(
            "PERIOD-OBSERVATION",
            self.workflow.table_observation_period_transformation,
            observation_period_cols,
            out_name="OBSERVATION_PERIOD",
        )
        df_drug = self.process_table(
            "DRUG_EXPOSURE",
            self.workflow.table_drug_exposure_transformation,
            drug_exposure_cols,
            out_name="DRUG_EXPOSURE",
        )
        df_procedure = self.process_table(
            "PROCEDURE_OCCURRENCE",
            self.workflow.table_procedure_occurrence_transformation,
            procedure_occurrence_cols,
            out_name="PROCEDURE_OCCURRENCE",
        )

        # -----------------------------
        # Step 2: Build VISIT_OCCURRENCE
        # -----------------------------
        print("\nBuilding VISIT_OCCURRENCE table...")

        # Collect all domain tables that may contain visit-related information
        tables_with_visit_info = [
            df_condition,
            df_measurement,
            df_observation,
            df_drug,
            df_procedure,
        ]

        visit_dfs = []
        for table in tables_with_visit_info:
            # Ensure all required VISIT_OCCURRENCE columns exist
            for col in visit_occurrence_cols:
                if col not in table.columns:
                    table[col] = None

            # Keep only VISIT_OCCURRENCE columns, preserving order
            visit_dfs.append(table[visit_occurrence_cols])

        # Concatenate visit-level info from all domain tables
        df_visit_occurrence = pd.concat(visit_dfs, ignore_index=True)

        # Drop duplicate rows if any
        df_visit_occurrence.drop_duplicates(inplace=True)

        df_visit_occurrence = self.workflow.table_visit_occurrence_transformation(df_visit_occurrence)

        # Save the VISIT_OCCURRENCE CSV
        os.makedirs("data", exist_ok=True)
        df_visit_occurrence.to_csv("data/VISIT_OCCURRENCE.csv", index=False, header=True)
        print("VISIT_OCCURRENCE table has been created → data/VISIT_OCCURRENCE.csv")

# ------------------------------------------------------------
# Script entry point
# ------------------------------------------------------------
if __name__ == "__main__":
    config = {
        "TRIPLESTORE_URL": "https://graphdb.ejprd.semlab-leiden.nl/repositories/unifiedCDE_model_no_context",
        "TRIPLESTORE_USERNAME": "pabloa",
        "TRIPLESTORE_PASSWORD": "ejprdejprd",
    }

    format_dir = "templates"
    c2o = CARE2OMOP(config, format_dir)
    c2o.run()