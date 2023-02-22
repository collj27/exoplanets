import time
import openai
import requests
from PIL import Image
import concurrent.futures
import boto3
from io import BytesIO

# Define the S3 bucket and object key
bucket_name = "dalle2-exoplanets"

# Create a new S3 client
s3 = boto3.client("s3")

#TODO: move to env file
openai.api_key = "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"


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


def call_dalle2(row):
    try:
        prompt = "The landscape of a {planet_type} that is {distance} from earth and is called {name}." \
                 "It has a mass of {mass} kilograms and radius of {radius} kilometers." \
            .format(planet_type=row['planet_type'], distance=row['distance'],
                    name=row['name'], mass=row['mass'], radius=row['radius']
                    )
        # Make the API request to DALL-E 2
        response = openai.Image.create(
            prompt=prompt,
            n=1,
            size="1024x1024"
        )

        # download image from url and upload to s3
        image = Image.open(requests.get(response['data'][0]['url'], stream=True).raw)
        file_name = row['name'] + ".jpg"
        upload_image(image, file_name)
    except Exception as e:
        print("There was a problem generating the image")
        print(e)


def generate_and_save_images(rows):
    print("Processing partition")
    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(call_dalle2, rows)

    print("Partition processed, sleeping for 60 second to ensure api limit isn't hit")
    time.sleep(60)
