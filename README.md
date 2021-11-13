# s3-tree-clone
Clone a filesystem tree to S3 (including metadata), skipping over files that are already synced,
in a manner compatible with AWS File Gateway and other S3-to-filesystem products.

## Usage

`s3-tree-clone [options] <src-dir> s3://<bucket>[/<prefix>]`

Copy the filesystem tree rooted at _src-dir_ to the given S3 destination.
If _prefix_ is non-empty, it will have a slash appended if necessary.

The _src-dir_ argument is interpreted similarly to rsync: if it ends with a `/`,
no directory is created in the S3 destination. If it does not end with a `/`,
the directory at the end of _src-dir_ is created.

### Options

* `-check-bucket`: Call `GetBucketLocation` to verify the bucket location. This will automatically
    switch to the destination region.
* `-encryption-algorithm AES256|aws:kms`: he S3 server-side encryption algorithm to use. This must be
    either `AES256` (default) or `aws:kms`.
* `-help`: Show this usage information.
* `-ignore-timestamps`: Ignore file timestamps when comparing files.
* `-kms-key <id>`: If `-encryption-algorithm` is `aws:kms`, the KMS key ID to use. Defaults to
    `aws/s3`.
* `-max-backoff-delay <duration>`: The maximum retry backoff delay. Specify a duration such as
    `1.5m`, `1m30s`, etc. Defaults to `60s`.
* `-max-concurrent <int>`: The maximum number of concurrent S3 requests to make. Defaults to 30.
* `-max-retries <int>`: The maximum number of retries for a single S3 request. Defaults to 10.
* `-profile <profile>`: The credentials profile to use.
* `-region <region>`: The AWS region to use. Defaults to `$AWS_REGION`, `$AWS_DEFAULT_REGION`,
    the configured region for the profile (if specified), or the instance region, whichever is
    appropriate.
* `-root-squash`: Change files owned by root to nfsnobody.
* `-storage-class <class>`: The S3 storage class to use. One of `STANDARD`, `STANDARD_IA`,
    `ONEZONE_IA`, `INTELLIGENT_TIERING`, `GLACIER`, `DEEP_ARCHIVE`, or `OUTPOSTS`. Defaults to
    `STANDARD`. `REDUCED_REDUNDANCY` has been deprecated and is not supported.
