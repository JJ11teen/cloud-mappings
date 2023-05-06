from functools import partial, reduce
from typing import Callable, List, NamedTuple


class CloudMappingSerialisation(NamedTuple):
    """A combination of a dumps and a loads function, to control serialisation of objects
    through a CloudMapping instance

    dumps : Callable
        Function to dump values through when writing to the cloud.
        Must return a bytes-like object.
    loads : Callable
        Function to load values through when reading from the cloud.
        Must expect a bytes-like object as its input.
    """

    dumps: Callable
    """Function to dump values through when writing to the cloud. Must return a bytes-like object."""
    loads: Callable
    """Function to load values through when reading from the cloud. Must expect a bytes-like object as its input."""

    def __bool__(self) -> bool:
        """A CloudMappingSerialisation is False only when both dumps and loads are None"""
        return not (self.dumps is None and self.loads is None)

    def __eq__(self, __value: object) -> bool:
        """A CloudMappingSerialisation is equal to another object if the other object is also a
        CloudMappingSerialisation, and both the repr() of both dumps and loads of each are equal"""
        if not isinstance(__value, CloudMappingSerialisation):
            return False
        return self.dumps.__repr__() == __value.dumps.__repr__() and self.loads.__repr__() == __value.loads.__repr__()

    @staticmethod
    def from_chain(
        ordered_dumps_funcs: List[Callable],
        ordered_loads_funcs: List[Callable],
    ) -> "CloudMappingSerialisation":
        """Creates a CloudMappingSerialisation by chaining consecutive dumps and loads functions together

        Parameters
        ----------
        ordered_dumps_funcs : List[Callable]
            An ordered list of functions to pass values through before saving bytes to the cloud.
            The last function must return a bytes-like object.
        ordered_loads_funcs : List[Callable]
            An ordered list of functions to pass values through before saving bytes to the cloud.
            The first function must expect a bytes-like object as its input.

        Returns
        -------
        CloudMappingSerialisation
            A CloudMappingSerialisation with the given dumps and loads functions chained together
        """

        def _apply(input, func):
            return func(input)

        return CloudMappingSerialisation(
            dumps=partial(reduce, _apply, ordered_dumps_funcs),
            loads=partial(reduce, _apply, ordered_loads_funcs),
        )


class BuiltinSerialisers:
    def pickle(protocol: int = None) -> CloudMappingSerialisation:
        """Serialiser that pickles values using pythons `pickle`

        Parameters
        ----------
        protocol : str, default=None
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

    def raw_string(encoding: str = "utf-8") -> CloudMappingSerialisation:
        """Serialiser that only encodes raw string values

        Returns
        -------
        CloudMappingSerialisation
            A CloudMappingSerialisation with pickle serialisation
        """
        return CloudMappingSerialisation(
            dumps=partial(bytes, encoding=encoding),
            loads=partial(str, encoding=encoding),
        )

    def json(encoding: str = "utf-8") -> CloudMappingSerialisation:
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

    def json_zlib(encoding: str = "utf-8") -> CloudMappingSerialisation:
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
