# hopeit.aws

`hopeit.engine` plugins for AWS.

This library is part of hopeit.engine:

> visit: https://github.com/hopeit-git/hopeit.engine

## `hopeit.aws.s3` plugin:

This library provides a `ObjectStorage` class to store and retrieve `@dataobjects` and files from S3 and compatible services as a plugin for the popular [`hopeit.engine`](https://github.com/hopeit-git/hopeit.engine) reactive microservices framework.

It supports the `prefix` setting, which is a prefix to be used for every element (object or file) stored in the S3 bucket. This prefix can be used to organize and categorize stored data within the bucket. Additionally, it supports the `partition_dateformat` setting, which is a date format string used to prefix file names for partitioning saved files into different subfolders based on the event timestamp (event_ts()). For example, using `%Y/%m/%d` will store each data object in a folder structure like year/month/day/, providing a way to efficiently organize and retrieve data based on date ranges. These settings can be used together to achieve more granular organization of data within the bucket.

### Installation

**hopeit.aws.s3**

Python library that provides helpers to store and retrieve `@dataobjects` and files to S3-compatible services

```bash
pip install hopeit.aws.s3
```
