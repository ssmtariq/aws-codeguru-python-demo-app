import aiofiles
import imageio
from skimage import exposure
from skimage import img_as_ubyte
from skimage.color import rgb2gray, rgba2rgb
from io import BytesIO


class ImageEditor:
    @staticmethod
    async def brighten_image(source_filename, target_filename):
        try:
            async with aiofiles.open(source_filename, 'rb') as source_file:
                image_data = await source_file.read()

            image = imageio.imread(BytesIO(image_data))
            brightened_image = exposure.adjust_gamma(image, 0.1)

            async with aiofiles.open(target_filename, 'wb') as target_file:
                await target_file.write(img_as_ubyte(brightened_image).tobytes())
        except Exception as e:
            print(f"Error in brighten_image: {e}")
            raise

    @staticmethod
    async def monochrome(source_filename, target_filename):
        try:
            async with aiofiles.open(source_filename, 'rb') as source_file:
                image_data = await source_file.read()

            image = imageio.imread(BytesIO(image_data))
            image_grey = rgb2gray(rgba2rgb(image))

            async with aiofiles.open(target_filename, 'wb') as target_file:
                await target_file.write(img_as_ubyte(image_grey).tobytes())
        except Exception as e:
            print(f"Error in monochrome: {e}")
            raise
