from typing import Union

from pandas import DataFrame


class TableParameters:
    def __init__(
        self,
        df: DataFrame,
        table_name: str,
        schema: str,
        method: str,
        id_field: Union[str, list],
    ):
        self.df = df
        self.table_name = table_name
        self.schema = schema
        self.method = method
        self.id_field = [id_field] if isinstance(id_field, str) else id_field
        # checks
        self.check_method()
        self.check_upsert()

    def check_method(self):
        valid_methods = ["create", "append", "upsert"]
        if self.method not in valid_methods:
            raise ValueError(f"No valid method given: {self.method}, " f"choose between {', '.join(valid_methods)}.")

    def check_upsert(self):
        if self.method == "upsert" and not self.id_field:
            raise ValueError("Id field not given while method is upsert.")
