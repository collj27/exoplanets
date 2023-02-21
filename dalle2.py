import openai
import requests
from PIL import Image
import os

openai.api_key = "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"


# Define a function to generate an image using DALL-E 2 and save it to a local directory
def generate_and_save_image(rows):
    for row in rows:
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

        # Get the URL of the generated image from the API response
        image_url = response['data'][0]['url']
        print(image_url)

        # TODO: save the images to s3 and urls in MongoDB, then display them in react app. Put in docker container and
        # deploy to aws

        # Download the image and save it to the local directory
        # image = Image.open(requests.get(image_url, stream=True).raw)
        # image.save(os.path.join('/Users/james/desktop/images/', row["name"] + "_" + row["planet_type"] + ".png"))
