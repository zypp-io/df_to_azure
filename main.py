import logging
import pandas as pd
from df_to_azure.export import run
from df_to_azure.log import set_logging
from datetime import datetime


if __name__ == "__main__":
    set_logging()
    logging.info(f"started script  at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # datasets
    tablename = "sample"
    df = pd.read_csv(
        f"/Users/melvinfolkers/Documents/github/df_to_azure/data/sample/{tablename}.csv"
    )
    schema = "tst"

    run(df, tablename, schema, incremental=True, id_field="col_a")
