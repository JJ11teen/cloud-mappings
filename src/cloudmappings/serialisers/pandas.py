from functools import partial

from pandas import DataFrame

from cloudmappings.serialisers import CloudMappingSerialisation


def csv() -> CloudMappingSerialisation[DataFrame]:
    """Serialiser that uses pandas to serialise DataFrames as csvs

    Returns
    -------
    CloudMappingSerialisation
        A CloudMappingSerialisation with csv serialisation
    """
    from io import BytesIO

    from pandas import read_csv

    return CloudMappingSerialisation.from_chain(
        ordered_dumps_funcs=[
            partial(DataFrame.to_csv, index=False),
            partial(bytes, encoding="utf-8"),
        ],
        ordered_loads_funcs=[
            BytesIO,
            partial(read_csv, encoding="utf-8"),
        ],
    )
