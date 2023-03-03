from io import BytesIO

import boto3

# Define the S3 bucket and object key
bucket_name = "dalle2-exoplanets"

# Create a new S3 client
s3 = boto3.client("s3")


def upload_image(image, file_name):
    try:
        # Save the PIL Image to a buffer
        buffer = BytesIO()
        image.save(buffer, format="JPEG")

        # Reset buffer position and upload to the S3 bucket
        buffer.seek(0)
        s3.upload_fileobj(buffer, bucket_name, file_name)
    except Exception as e:
        print("There was a problem uploading {0} to s3".format(file_name))
        print(e)
