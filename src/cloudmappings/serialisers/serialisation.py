from dataclasses import dataclass
from functools import partial, reduce
from typing import Callable, Generic, List, TypeVar

T = TypeVar("T")


@dataclass(frozen=True)
class CloudMappingSerialisation(Generic[T]):
    """A combination of a dumps and a loads function, to control serialisation of objects
    through a CloudMapping instance

    dumps : Callable
        Function to dump values through when writing to the cloud.
        Must return a bytes-like object.
    loads : Callable
        Function to load values through when reading from the cloud.
        Must expect a bytes-like object as its input.
    """

    dumps: Callable[[T], bytes]
    """Function to dump values through when writing to the cloud. Must return a bytes-like object."""
    loads: Callable[[bytes], T]
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
    ) -> "CloudMappingSerialisation[T]":
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
