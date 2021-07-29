class CreateContainerError(Exception):
    pass


class CreateSchemaError(Exception):
    pass


class EnvVariableNotSetError(Exception):
    """Error when required env variable(s) is not set"""

    pass


class PipelineRunError(Exception):
    """Error when ADF pipleine fails"""

    pass
