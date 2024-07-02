"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

import json
from typing import List, ClassVar

# TODO: delete redundant methods


class Namespace:
    """A namespace is a sequence of levels separated by dots. It's used to identify a metalake, a
    catalog or a schema. For example, "metalake1", "metalake1.catalog1" and
    "metalake1.catalog1.schema1" are all valid namespaces.
    """

    _DOT: ClassVar[str] = "."

    _levels: List[str]

    def __init__(self, levels: List[str]):
        self._levels = levels

    def to_json(self):
        return json.dumps(self._levels)

    @classmethod
    def from_json(cls, levels):
        assert levels is not None and isinstance(
            levels, list
        ), f"Cannot parse name identifier from invalid JSON: {levels}"
        return cls(levels)

    @staticmethod
    def empty() -> "Namespace":
        """Get an empty namespace.

        Returns:
            An empty namespace
        """
        return Namespace([])

    @staticmethod
    def of(*levels: str) -> "Namespace":
        """Create a namespace with the given levels.

        Args:
            levels The levels of the namespace

        Returns:
            A namespace with the given levels
        """
        Namespace.check(
            levels is not None, "Cannot create a namespace with null levels"
        )
        if len(levels) == 0:
            return Namespace.empty()

        for level in levels:
            Namespace.check(
                level is not None and level != "",
                "Cannot create a namespace with null or empty level",
            )

        return Namespace(list(levels))

    def levels(self) -> List[str]:
        """Get the levels of the namespace.

        Returns:
            The levels of the namespace
        """
        return self._levels

    def level(self, pos: int) -> str:
        """Get the level at the given position.

        Args:
            pos: The position of the level

        Returns:
            The level at the given position
        """
        if pos < 0 or pos >= len(self._levels):
            raise ValueError("Invalid level position")
        return self._levels[pos]

    def length(self) -> int:
        """Get the length of the namespace.

        Returns:
            The length of the namespace.
        """
        return len(self._levels)

    def is_empty(self) -> bool:
        """Check if the namespace is empty.

        Returns:
            True if the namespace is empty, false otherwise.
        """
        return len(self._levels) == 0

    def __eq__(self, other: "Namespace") -> bool:
        if not isinstance(other, Namespace):
            return False
        return self._levels == other._levels

    def __hash__(self) -> int:
        return hash(tuple(self._levels))

    def __str__(self) -> str:
        return self._DOT.join(self._levels)

    @staticmethod
    def check(expression: bool, message: str, *args) -> None:
        """Check the given condition is true. Throw an IllegalNamespaceException if it's not.

        Args:
            expression: The expression to check.
            message: The message to throw.
            args: The arguments to the message.
        """
        if not expression:
            raise ValueError(message.format(*args))
