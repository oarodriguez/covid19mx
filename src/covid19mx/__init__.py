"""covid19mx - A template for your Python project.

Copyright © 2023, Omar Abel Rodríguez-López.
"""

import importlib.metadata as importlib_metadata

metadata = importlib_metadata.metadata("covid19mx")

# Export package information.
__version__ = metadata["version"]
__author__ = metadata["author"]
__description__ = metadata["description"]
__license__ = metadata["license"]

__all__ = [
    "__author__",
    "__description__",
    "__license__",
    "__version__",
    "metadata",
]
