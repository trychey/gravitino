"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from abc import ABC, abstractmethod

from typing import List

from gravitino.api.fileset import Fileset


class FilesetContext(ABC):
    """An interface representing a fileset context with an existing fileset. This
    interface defines some contextual information related to Fileset that can be passed.
    """

    @abstractmethod
    def fileset(self) -> Fileset:
        """The fileset object.

        Returns:
            the fileset object.
        """
        pass

    @abstractmethod
    def actual_paths(self) -> List[str]:
        """The actual storage paths after processing.

        Returns:
            the actual storage paths after processing.
        """
        pass
