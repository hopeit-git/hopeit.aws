Release Notes
=============

Version 0.1.2
_____________
- hopeit.aws.s3 

   - Fix: Better `prefix` handling in `get_files`, `list_files`, and `list_objects`. Consistent location return on `store` and `store_file`.

Version 0.1.1
_____________
- hopeit.aws.s3 

   - Fix: missing `prefix` handling in `get_file_chunked`

Version 0.1.0
_____________
- hopeit.aws.s3 (initial release):

  - `ObjectStorage` class to store and retrieve `@dataobjects` and files from S3 and compatible services.

  - Support for `prefix` setting to organize and categorize stored data within the S3 bucket.

  - Support for `partition_dateformat` setting to partition saved files into different subfolders based on the event timestamp.