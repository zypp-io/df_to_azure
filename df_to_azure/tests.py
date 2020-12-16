from df_to_azure.export import run, run_multiple
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv(verbose=True, override=True)


def test_create():

    run(
        df=pd.read_csv("../data/sample_1.csv"),
        tablename="sample",
        schema="test",
        method="create",
        id_field="col_a",
    )


def test_upsert():

    run(
        df=pd.read_csv("../data/sample_2.csv"),
        tablename="sample",
        schema="test",
        method="upsert",
        id_field="col_a",
    )


def test_run_multiple():

    df_dict = dict()
    for file in os.listdir("../data"):
        if file.endswith(".csv"):
            df_dict[file.split(".csv")[0]] = pd.read_csv(os.path.join("data", file))

    run_multiple(df_dict, schema="test", method="create")


if __name__ == "__main__":
    test_create()
    test_upsert()
    test_run_multiple()
