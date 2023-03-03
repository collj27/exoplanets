import os
import time
import openai
import requests
from PIL import Image
import concurrent.futures
import sys
from s3 import upload_image

openai.api_key = os.env["OPEN_AI_API_KEY"]


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
        file_name = row['planet_type'] + '_' + row['name'] + ".jpg"
        upload_image(image, file_name)
    except openai.error.OpenAIError as e:
        print(e.http_status)
        print(e.error)
        print("exiting thread")
        sys.exit()


def generate_and_save_images(rows):
    print("Processing partition")
    # generate images in each partition concurrently
    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(call_dalle2, rows)

    print("Partition processed, sleeping for 60 second to ensure api limit isn't hit")
    time.sleep(60)
