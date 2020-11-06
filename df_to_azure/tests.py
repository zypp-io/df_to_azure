from df_to_azure.export import run, run_multiple, download_blob
from df_to_azure.parse_settings import TableParameters
import pandas as pd
import os


YAML_PATH = os.path.join("..", "azure.yml")


def test_create():

    run(
        df=pd.read_csv("data/sample_1.csv"),
        tablename="sample",
        schema="test",
        method="create",
        id_field="col_a",
        cwd=YAML_PATH,
    )


def test_upsert():

    run(
        df=pd.read_csv("data/sample_2.csv"),
        tablename="sample",
        schema="test",
        method="upsert",
        id_field="col_a",
        cwd=YAML_PATH,
    )


def test_run_multiple():

    df_dict = dict()
    for file in os.listdir("data"):
        if file.endswith(".csv"):
            df_dict[file.split(".csv")[0]] = pd.read_csv(os.path.join("data", file))

    run_multiple(df_dict, schema="test", method="create", yaml_path=YAML_PATH)


if __name__ == '__main__':
    table = TableParameters(
        df=pd.read_csv("../data/sample_2.csv"),
        name="sample",
        schema="test",
        method="upsert",
        id_field="col_a",
        cwd=YAML_PATH,
    )
    download_blob(table)