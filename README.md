# RPM S3 mirror

Tool to create a RPM mirror in s3 and periodically sync changes.

This tool is currently in alpha.

### Configuration

The tool can be configured either with a config file or via environment variables. The following options are supported:

*aws_access_key_id* - AWS access key id 

*aws_secret_access_key* - AWS access key

*bucket_name* - name of the bucket to sync to

*bucket_region* - region bucket is in

*max_workers* - number of worker threads to use during sync

*upstream_repositories* - list of upstream repositories to sync

*scratch_dir* - where to cache files during sync (defaults to /var/tmp)

### Manifest

On a successful sync, changed packages and some additional metadata are put in the `manifests` directory. Additionally, the previous repomd.xml file is also stored there.

### Metrics

The tool emits some simple metrics in the statsd format (https://github.com/influxdata/telegraf/tree/master/plugins/inputs/statsd)

### Example

```
python3 -m rpm_s3_mirror --config config.json
```

where config.json is something like:
```
{
  "aws_access_key_id": "***",
  "aws_secret_access_key": "***",
  "bucket_name": "aiven-willcoe-test",
  "bucket_region": "ap-southeast-2",
  "upstream_repositories": [
    "https://dl01.fedoraproject.org/pub/fedora/linux/updates/31/Modular/x86_64/", "https://dl01.fedoraproject.org/pub/fedora/linux/releases/31/Modular/x86_64/os/", "https://dl01.fedoraproject.org/pub/fedora/linux/releases/31/Everything/x86_64/os/", "https://dl01.fedoraproject.org/pub/fedora/linux/updates/31/Everything/x86_64/"
  ]
}
```
