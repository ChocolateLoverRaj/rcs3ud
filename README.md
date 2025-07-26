# rcs3ud
Reliable, cheap S3 uploader and downloader

## Reliable
- Retries, even after a program restart
- No `unwrap`s

## Cheap
- Specify a monthly limit so you don't have to pay for high internet usage
- Optimaly restores and downloads from S3 glacier
- Does not use multi-part uploads

## S3
Made for AWS, but it should work on any S3-compatible service. Contributions for other services welcome.

## Uploader
Uploads files.

## Downloader
Downloads files, restoring archived files.

## Features
### General
- [x] Gracefully handles errors and retries when uploading

### Upload
- [x] Upload files within the limit (5GB for AWS)
- [x] Upload from custom `Stream`s
- [x] Specify times to upload (so you can upload when you aren't gaming)
- [x] Limit monthly upload amounts (if your internet has a monthly limit)
- [ ] Reports progress (currently not possible because of a limitation in the AWS Rust library)
- [ ] Upload a large file as multiple S3 objects (planned)

### Download
- [x] Resume a download operation after the program (or system) restarts
- [x] Download from cold storage
- [x] Reports progress
- [ ] Mechanism to stay within the AWS Free Tier limit for data out from AWS (planned)
- [x] Limit monthly download amounts (if your internet has a monthly limit)
- [ ] Download a large file that's stored as multiple S3 objects (planned)
- [ ] When downloading a restored object, copies the object to a `STANDARD` tier object if the restore is about to expire (planned)
- [ ] Schedules a restored object to be automatically copied to a `STANDARD` tier object before the restore expires, as a S3 job, so that even if your program doesn't run locally, the object will be copied (could be implemented)
- [ ] Use AWS SQS to cheaply frequently poll for an object being restored (could be implemented)
- [ ] Use AWS SNS to send a REST request to a local server when an object is restored (could be implemented)

## Why no multi-part uploads
This tool was created to upload (and if needed, download) backups of ZFS datasets, which could be up to 700 GB in size, into the AWS `DEEP_ARCHIVE` tier. `DEEP_ARCHIVE` is cheap to store ($1/TB/month in 2025), but if you use multi-part uploads, the upload cost will be huge because you will be billed at the `STANDARD` tier while your upload is in progress, and it will take a **long** time to upload 700 GB. Also when restoring it will be very expensive because every time you need to download a chunk of the object, you will need to have the entire object restored, which again is charged at the `STANDARD` tier, plus there would be extra restore costs.

## Goals
1. Be able to restore data

Since this tool was created to backup files, the most important priority is to not end up in a situation where you think a file is backed up to AWS, but it was actually corrupted by the uploader program, or you try to restore the backup, but the program fails to download it (or it's not implemented). This is why this program does not do any special modifications. After uploading, you can verify just by comparing the hash from the file with the hash on AWS. If the file gets split into multiple files, it's still just simple chunking and you can verify by checking hashes manually.

2. It should work

When you're uploading 700 GB files, there is definitely going to be a network error somewhere in there. And the sys admin shouldn't have to deal with errors manually. That is why this program keeps retrying network and service errors. Currently, it doesn't retry file errors, but this could change in the future.

3. It should not interfere with your other things

This tool should not hog all of your bandwidth. So your family should be able to watch videos, browse the internet, do video calls, and play video games while this program is running. This is done by limiting uploads to be within certain hours, so you can configure this to be when you are sleeping or out of the house. This way, you can play video games even on the same machine that's running this program, and this program will not be a source of lag to blame when you lose in your game.

My home internet has a monthly limit, and backup could interfere with this. That's why there is a monthly upload amount limit feature, which will pause uploads until the next month.

4. It should be the cheapest

Currently, the cheapest cloud backup solution is AWS `DEEP_ARCHIVE`. The cheapest way to store files is to combine all of the files into a single file, and then upload that file into 5 GB chunks. I do this by using ZFS and uploading ZFS incremental snapshot files produced by `zfs send`.

5. It should be fast

Currently this shouldn't be *slow* but it isn't really *optimized* to be the fasted. It's more optimized for low cost and simplicity (so less room for bugs).
