Release Notes
=============

Version 0.2.0
_____________
- hopeit.aws.s3 

   - Updated to ensure compatibility with `hopeit.engine` version 0.25.0.
   
   BREAKING CHANGES
   ================

   - This version introduces breaking changes and is not backwards compatible with any previous versions of `hopeit.engine`.
   Please upgrade `hopeit.engine` to version 0.25.0 or later to use this release.

   - The `list_files` and `list_objects` methods now require the `recursive` parameter to be passed as a named argument.

Version 0.1.3
_____________
- hopeit.aws.s3 

   - Extracted bucket creation logic from settings into a separate method `bucket_creation`.
   - Set default listing behavior to non-recursive, treating subdirectories as part of the key; partitions only show the partition part of the key.
   - Use `_build_key` method internally to handle prefix and partitioning part of the key.
   - In the `aws_example` app, moved `create_bucket` from endpoint initialization to an initialization SETUP event.

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