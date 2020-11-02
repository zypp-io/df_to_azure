import logging
import pandas as pd
from df_to_azure.export import run
from df_to_azure.log import set_logging
from datetime import datetime
import time

if __name__ == "__main__":
    set_logging()
    logging.info(f"started script  at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # testing.

    run(
        df=pd.read_csv("data/sample/sample_1.csv"),
        tablename="sample",
        schema="test",
        method="create",
        id_field="col_a",
    )

    time.sleep(5)

    run(
        df=pd.read_csv("data/sample/sample_2.csv"),
        tablename="sample",
        schema="test",
        method="upsert",
        id_field="col_a",
    )
