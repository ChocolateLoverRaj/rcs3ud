# rcs3ud
Reliable, cheap S3 uploader and downloader

## Reliable
- Retries, even after a program restart
- No `unwrap`s

## Cheap
- Specify a monthly limit so you don't have to pay for high internet usage
- Optimaly restores and downloads from S3 glacier
- Does not use multi-part uploads (so 5 GB size limit for now)

## S3
Made for AWS, but it should work on any S3-compatible service. Contributions for other services welcome.

## Uploader
Uploads files.

## Downloader
Downloads files, restoring archived files.

## Features (in progress)
- [x] Upload a file to S3
- [x] Gracefully handles errors and retries when uploading
- [x] Specify times to upload (so you can upload when you aren't gaming)
- [ ] Limit monthly upload amounts (if your internet has a monthly limit)
- [x] Download a file from S3
- [x] Download a file from S3 glacier
- [x] Resume a download after the program (or system) restarts
- [ ] Specify a monthly download limit to stay within the AWS Free Tier limit
- [ ] Reports progress of an upload (currently not possible because of a limitation in the AWS Rust library)
- [x] Reports progress of a download
