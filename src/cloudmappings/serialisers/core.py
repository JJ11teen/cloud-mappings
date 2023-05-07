from functools import partial
from typing import Any

from cloudmappings.serialisers import CloudMappingSerialisation


def none() -> CloudMappingSerialisation[bytes]:
    """This serialiser performs no serialisation, and passes raw bytes

    It is implemented as `None`, and `None` can be used directly, however
    using this serialiser enables type hints to correctly pick up `bytes`
    as the mapping value.

    Returns
    -------
    CloudMappingSerialisation
        None, typed as CloudMappingSerialisation[bytes]
    """
    return None


def pickle(protocol: int = None) -> CloudMappingSerialisation[Any]:
    """Serialiser that pickles values using pythons `pickle`

    Parameters
    ----------
    protocol : int, default=None
        The pickle protocol to use, defaults to None which internally defaultto `pickle.DEFAULT_PROTOCOL`

    Returns
    -------
    CloudMappingSerialisation
        A CloudMappingSerialisation with pickle serialisation
    """
    import pickle

    return CloudMappingSerialisation(
        dumps=partial(pickle.dumps, protocol=protocol),
        loads=pickle.loads,
    )


def raw_string(encoding: str = "utf-8") -> CloudMappingSerialisation[str]:
    """Serialiser that only encodes raw string values

    Parameters
    ----------
    encoding : str, default="utf-8"
        The string encoding to use, passed to bytes() and str() for dumps and loads respectively.

    Returns
    -------
    CloudMappingSerialisation
        A CloudMappingSerialisation with pickle serialisation
    """
    return CloudMappingSerialisation(
        dumps=partial(bytes, encoding=encoding),
        loads=partial(str, encoding=encoding),
    )


def json(encoding: str = "utf-8") -> CloudMappingSerialisation[Any]:
    """Serialises values to JSON strings

    Parameters
    ----------
    encoding : str, default="utf-8"
        The string encoding to use, passed to bytes() and str() for dumps and loads respectively.

    Returns
    -------
    CloudMappingSerialisation
        A CloudMappingSerialisation for JSON string serialisation
    """
    import json

    return CloudMappingSerialisation.from_chain(
        ordered_dumps_funcs=[partial(json.dumps, sort_keys=True), partial(bytes, encoding=encoding)],
        ordered_loads_funcs=[partial(str, encoding=encoding), json.loads],
    )


def json_zlib(encoding: str = "utf-8") -> CloudMappingSerialisation[Any]:
    """Serialises values to compressed JSON strings

    Uses zlib to compress values after serialising them JSON strings.

    Parameters
    ----------
    encoding : str, default="utf-8"
        The string encoding to use, passed to bytes() and str() for dumps and loads
        respectively.

    Returns
    -------
    CloudMappingSerialisation
        A CloudMappingSerialisation for zlib compression and JSON string serialisation
    """
    import json
    import zlib

    return CloudMappingSerialisation.from_chain(
        ordered_dumps_funcs=[
            partial(json.dumps, sort_keys=True),
            partial(bytes, encoding=encoding),
            zlib.compress,
        ],
        ordered_loads_funcs=[
            zlib.decompress,
            partial(str, encoding=encoding),
            json.loads,
        ],
    )
