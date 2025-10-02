import os
import pandas as pd
from utils import Workflow
from cols import (
    person_cols,
    death_cols,
    condition_occurrence_cols,
    measurement_cols,
    observation_cols,
    drug_exposure_cols,
    procedure_occurrence_cols,
    visit_occurrence_cols,
)

class CARE2OMOP:
    def __init__(self, endpoint, format_dir):
        self.workflow = Workflow(endpoint, format_dir)

    def process_table(self, name, transform_func, cols, out_name=None):
        """Extrae, transforma y guarda una tabla OMOP. Devuelve el DataFrame resultante (post-selección)."""
        df = self.workflow.extract_table(name)
        df = transform_func(df)
        if cols:
            selected = [c for c in df.columns if c in cols]
            if selected:
                df_out = df[selected].copy()
            else:
                df_out = pd.DataFrame(columns=cols)
        else:
            df_out = df
        out_name = out_name or name
        out_path = f"data/{out_name}.csv"
        os.makedirs("data", exist_ok=True)
        df_out.to_csv(out_path, index=False)
        print(f"{out_name} table created → {out_path}")
        return df_out

    def run(self):
        # Process core tables and keep transformed DataFrames to build VISIT
        df_person = self.process_table("PERSON", self.workflow.table_person_transformation, person_cols, out_name="PERSON")
        df_death = self.process_table("DEATH", self.workflow.table_death_transformation, death_cols, out_name="DEATH")
        df_condition = self.process_table("CONDITION", self.workflow.table_condition_transformation, condition_occurrence_cols, out_name="CONDITION")
        df_measurement = self.process_table("MEASUREMENT", self.workflow.table_measurement_transformation, measurement_cols, out_name="MEASUREMENT")
        df_observation = self.process_table("OBSERVATION", self.workflow.table_observation_transformation, observation_cols, out_name="OBSERVATION")
        df_drug = self.process_table("DRUG_EXPOSURE", self.workflow.table_drug_exposure_transformation, drug_exposure_cols, out_name="DRUG_EXPOSURE")
        df_procedure = self.process_table("PROCEDURE_OCCURRENCE", self.workflow.table_procedure_occurrence_transformation, procedure_occurrence_cols, out_name="PROCEDURE_OCCURRENCE")

        # -----------------------------
        # Crear tabla VISIT_OCCURRENCE
        # -----------------------------
        visit_dfs = []

        # Lista de tablas que aportan información de visitas
        tables_with_visit_info = [
            df_condition,
            df_measurement,
            df_observation,
            df_drug,
            df_procedure
        ]

        for table in tables_with_visit_info:
            # Asegurarse de que todas las columnas necesarias existan
            for col in visit_occurrence_cols:
                if col not in table.columns:
                    table[col] = None

            # Seleccionar columnas en el orden de visit_occurrence_cols
            visit_dfs.append(table[visit_occurrence_cols])

        # Concatenar todas las filas de las distintas tablas
        df_visit_occurrence = pd.concat(visit_dfs, ignore_index=True)

        # Deduplicar si hay filas repetidas
        df_visit_occurrence.drop_duplicates(inplace=True)

        # Guardar CSV final
        df_visit_occurrence.to_csv("data/VISIT_OCCURRENCE.csv", index=False, header=True)
        print("VISIT_OCCURRENCE table has been created")


if __name__ == "__main__":
    endpoint = "http://localhost:7200/repositories/care-sm"
    format_dir = "templates"
    c2o = CARE2OMOP(endpoint, format_dir)
    c2o.run()
