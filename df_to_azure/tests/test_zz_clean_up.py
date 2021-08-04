from df_to_azure.db import auth_azure


# --- CLEAN UP ----
def test_clean_up_db():
    tables_dict = {
        "covid": ["covid_19"],
        "staging": ["category", "employee_1", "sample", "sample_spaces_column_name"],
        "test": [
            "category",
            "category_1",
            "category_2",
            "employee_1",
            "employee_2",
            "employee_duplicate_keys",
            "employee_duplicate_keys_1",
            "employee_duplicate_keys_2",
            "sample",
            "sample_1",
            "sample_2",
            "sample_3",
            "sample_spaces_column_name",
            "test_df_to_azure",
            "wrong_method",
            "long_string",
            "quote_char",
            "append",
            "pipeline_name",
            "bigint",
            "bigint_convert",
        ],
    }

    with auth_azure() as con:
        with con.begin():
            for schema, tables in tables_dict.items():
                for table in tables:
                    query = f"DROP TABLE IF EXISTS {schema}.{table};"
                    con.execute(query)
