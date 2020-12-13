from pandas import DataFrame
from typing import Union


class TableParameters:
    def __init__(
        self,
        df: DataFrame,
        name: str,
        schema: str,
        method: str,
        id_field: Union[str, list],
        cwd: str,
    ):
        self.df = df
        self.name = name
        self.schema = schema
        self.method = method
        self.id_field = [id_field] if isinstance(id_field, str) else id_field
        self.cwd = cwd
