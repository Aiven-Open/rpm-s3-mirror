# POC Fedora S3 mirror tool

This repo contains the initial POC implementation of a tool to create and periodically sync upstream Fedora repositories with s3.

Q: why write a new tool and not use an existing one?
A: I didn't see any tools to do the job. There are existing tools for mirroring Fedora repositories but the recommended one is written in elite bash and requires a persistent VM. I wanted ideally to have a tool with no persistent state to more easily fit into our existing infrastructure (makes everything much easier).

Q: how does it work?
A: the idea is to periodically parse the upstream repomd.xml files and diff it with what we have in our mirror. Any changes are then synced across to s3. We never delete anything.

Q: why is this in a new repo and not aiven core? 
A: I thought this would be a nice self contained thing we could release as open source.

Q: what about versioning/snapshotting like mentioned in https://app.clubhouse.io/aiven/story/12173/private-mirror-for-fedora-repositories#activity-12352
A: snapshotting should be possible using the "metalink" feature and a lambda function/api endpoint. Can add later.

Q: what is the resource usage like?
A: on initial sync it can chew a fair bit of network/CPU/memory. This can be throttled somewhat by using less workers.
 
Q: how do I run this thing?
A: you can run it as follows:

```
python3 -m fedora_s3_mirror --config config.json
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