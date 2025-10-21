## Monthly Data Usage Limits
Most internet service providers have monthly limits on how much data you can upload and download. Because our program might cause us to reach this limit, our program needs to be aware of this and throttle its own monthly data usage. This means postponing uploads and downloads to the next month.

The limit that I have seen is from Xfinity, which is a 1.2 (or 1.3, I'm not sure) TB limit which resets at the first day of every month. Other plans may reset every 30 days or something, I'm not sure.

## Assumptions
We will assume that the ISP's data limit is based on a synchronized clock and there is no variation in when the limit is reset. We will assume it is reset exactly at midnight. However, this may differ from real life behavior.

We will assume that data usage is measured per IP packet and not per TCP or HTTP connection. So if we do a 2 GB upload that goes through midnight and the first 1 GB was uploaded before midnight and the second 1 GB was uploaded after midnight, the ISP would count 1GB for one day and 1 GB for the next day, and not all 2 GB as the next day.

We will assume that if we are about to hit the monthly limit, and we are uploading a big file, and if the monthly limit gets reset in the middle of the file upload, then we can upload the rest of the file without the connection being disconnected due to reaching the monthly limit.

## Keeping track of data usage
When we upload an object using a PUT request, the AWS Rust client does not tell us detailed information about how much of it was sent at what time. So if a PUT request goes over the monthly reset point, we will assume that its data usage counted for both the previous month and current month after it's completed.

If a PUT request fails in the middle of uploading an object, we won't know how much bandwidth it used, so we will assume that it used 100% of the object's size.

Our application cannot be 100% sure about how much data it actually used because of IP, TCP, HTTP, and possible VPN overhead. We can adjust for this by assuming a certain per-request overhead for the headers, URL, etc. We can also adjust multiplier, so if we upload 100 GB of files, we could assume we used 105 GB if there was a 1.05 multiplier.

So, to conclude, if a request fails before the file started uploading, we only need to mark the metadata of the request as used. If a request fails after it started uploading, or if a request succeeds, then we count the request metadata plus the object size. We factor in the start and end time of the request, and multiply by the multiplier.

## Limiting data usage
There are probably other devices in the house that will use data that gets counted towards the monthly limit. Unless we can somehow get the current data usage with our program, we can just "allocate" up to a certain limit for our program to use every month. If the limit is 1.2 TB, we can allocate 300 GB every month to be used by our program.

Before we upload an object, we can estimate how much data it will use. We can overestimate to be safe. Then, we can make sure that out of our self-imposed monthly limit (300 GB in the example) will not be exceeded by uploading that object. If the current month doesn't have enough data left for the upload, we can schedule the upload for as soon as the monthly limit is reset.

## Notifications about Throttling
- Backup postponed due to data limit
- If >=100% of the monthly capacity is scheduled for next month. For example, if the current month's limit is used up, and the next month is already fully "booked".
