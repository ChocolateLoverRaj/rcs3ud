Ideally, you have a program scheduled to continuously back up ZFS datasets to S3, so that no manual intervention is needed. All the user has to do is check that everything is working once in a while. We can make checking easier by notifying / emailing the user a monthly summary, which can include information about the amount of data uploaded, the amount of scheduled uploads for next month, and any errors. If a non-retry-able error happens, that is serious, and it is best if a notification was sent right away.

We could use ntfy to send notifications
