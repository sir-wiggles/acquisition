## Acquisition

The main goal of this program is to take the content a publisher uploads to our FTP server and expand the content into another location.
While expanding the content find pairs (meta <--> content) and send a message to a queue to be ingested into DynamoDB

There is no specific format that publishers follow, but it looks like publishers are consistent with the format that they use.

## Building

go build 

## Running 

./acquisition

    -ftp-bucket string 
        Where the publisher supplied content is located

    -key string
        AWS S3 Key. If not supplied, it will look for certs in the .aws credentials file

    -secret string
        AWS S3 Secret. If not supplied, it will look for certs in the .aws credentials file

    -new-content-queue string
        Name of the queue where new content messages are put from SNS

    -processed-bucket string
        Where the expanded content should be placed

    -processed-queue string
        Name of the quere there the pairs should be sent.

    -region string
        AWS region

    -workers int
        Number of workers to run at a time.


