# hopeit.aws.s3 plugin

This library is part of hopeit.engine:

> visit: https://github.com/hopeit-git/hopeit.engine

This library provides a `ObjectStorage` class to store and retrieve `@dataobjects` and files from S3 and compatible services as a plugin for the popular [`hopeit.engine`](https://github.com/hopeit-git/hopeit.engine) reactive microservices framework.

It supports the `prefix` setting, which is a prefix to be used for every element (object or file) stored in the S3 bucket. This prefix can be used to organize and categorize stored data within the bucket. Additionally, it supports the `partition_dateformat` setting, which is a date format string used to prefix file names for partitioning saved files into different subfolders based on the event timestamp (event_ts()). For example, using `%Y/%m/%d` will store each data object in a folder structure like year/month/day/, providing a way to efficiently organize and retrieve data based on date ranges. These settings can be used together to achieve more granular organization of data within the bucket.

### Installation

Python library that provides helpers to store and retrieve `@dataobjects` and files to S3-compatible services

```bash
pip install hopeit.aws.s3
```

### Usage

```python
from hopeit.dataobjects import dataobject, dataclass
from hopeit.aws.s3 import ObjectStorage, ObjectStorageSettings, ConnectionConfig

# Create a connection configuration
conn_config = ConnectionConfig(
    aws_access_key_id="your-access-key-id",
    aws_secret_access_key="your-secret-access-key",
    region_name="your-region-name"
)

# Create settings for ObjectStorage
settings = ObjectStorageSettings(
    bucket="your-bucket-name",
    connection_config=conn_config
)

# Create an ObjectStorage instance
storage = ObjectStorage.with_settings(settings)

# Connect to the ObjectStorage
await storage.connect()

# hopeit.engine data object
@dataobject
@dataclass
class Something:
    key: str
    value: str

something = Something(key="my_key", value="some_value")

# Store a data object
await storage.store(key=something.key, value=something)

# Retrieve a data object
retrieved_object = await storage.get(key=something.key, datatype=Something)
print(retrieved_object)
```

## Example Usage

In the `apps/examples/aws-example/` directory, you can find a full example `hopeit.engine` app that demonstrates the usage of the `hopeit.aws.s3` plugin within the `hopeit.engine` framework. This example showcases how to store and retrieve `@dataobjects` and files from S3 using the `ObjectStorage` class.
