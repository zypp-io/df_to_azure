import logging
import os

from keyvault import secrets_to_environment
from pandas import read_csv

logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)
secrets_to_environment(keyvault_name="df-to-azure")

"""
This is the testing suite for df_to_azure. In general the following steps will be done per test:

1. Read a sample csv file from repo or the web
2. Write file to DB with df_to_azure, this can be create or upsert.
3. Use `wait_till_pipeline_is_done` function to wait for ADF pipeline
     to be Succeeded before continuing.
4. Read data back from DB and test if we got expected output
     with pandas._testing.assert_frame_equal.

NOTE: To keep the testing lightweight, we don't import whole modules but just the methods we need.
        like DataFrame from pandas.
"""

files = [
    "category_1",
    "category_2",
    "employee_1",
    "employee_2",
    "employee_duplicate_keys_1",
    "employee_duplicate_keys_2",
    "sample_1",
    "sample_2",
    "sample_3",
]

data = {file: read_csv(os.path.join("data", file + ".csv")) for file in files}
