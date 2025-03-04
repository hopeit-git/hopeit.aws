"""
Hopeit AWS S3 ObjectStorage API
"""

__version__ = "0.3.0rc0"

from hopeit.aws.s3.object_storage import (
    ConnectionConfig,
    ItemLocator,
    ObjectStorage,
    ObjectStorageSettings,
)

__all__ = ["ConnectionConfig", "ItemLocator", "ObjectStorage", "ObjectStorageSettings"]
