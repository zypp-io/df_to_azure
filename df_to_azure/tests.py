from df_to_azure import df_to_azure, dfs_to_azure
import pandas as pd
import os
from dotenv import load_dotenv
import logging

logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)
load_dotenv(verbose=True, override=True)


def test_create_1():
    df_to_azure(
        df=pd.read_csv("../data/sample_1.csv"),
        tablename="sample",
        schema="test",
        method="create",
        id_field="col_a",
    )


def test_create_2():
    df_to_azure(
        df=pd.read_csv("../data/category_1.csv"),
        tablename="category",
        schema="test",
        method="create",
        id_field="category_id",
    )


def test_upsert_1():
    df_to_azure(
        df=pd.read_csv("../data/sample_2.csv"),
        tablename="sample",
        schema="test",
        method="upsert",
        id_field="col_a",
    )


def test_upsert_2():
    df_to_azure(
        df=pd.read_csv("../data/category_2.csv"),
        tablename="category",
        schema="test",
        method="upsert",
        id_field="category_id",
    )


def test_run_multiple():

    df_dict = dict()
    for file in os.listdir("../data"):
        if file.endswith(".csv"):
            df_dict[file.split(".csv")[0]] = pd.read_csv(os.path.join("data", file))

    dfs_to_azure(df_dict, schema="test", method="create")


if __name__ == "__main__":
    test_create_2()
    # test_upsert_2()
    # test_run_multiple()
