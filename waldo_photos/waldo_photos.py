from multiprocessing import Process, Queue
from PIL import Image, ExifTags
import requests
from lxml import etree
from io import BytesIO
from urllib.parse import urljoin
from datetime import datetime
from pymongo import MongoClient
import argparse

def parse_data(url, queue):
    response = requests.get(url, stream=True)
    response.raw.decode_content = True
    for _, element in etree.iterparse(response.raw, tag='{*}Contents'):
        image_url = urljoin(url, element.findtext('{*}Key'))
        queue.put({'id': element.findtext('{*}ETag')[1:-1], 'url': image_url})

def image_exif_worker(queue, db, collection):
    collection = get_mongo_collection(db, collection)
    while True:
        image = queue.get()
        if image is None:
            break
        response = requests.get(image['url'])
        try:
            img = Image.open(BytesIO(response.content))
            try:
                exif = {
                    ExifTags.TAGS[k]: str(v)
                    for k, v in img._getexif().items()
                    if k in ExifTags.TAGS
                }
            except AttributeError:
                exif = {}
            try:
                collection.insert_one({'_id': image['id'], 'exif': exif})
            except:
                print("Skipping duplicate image: {}".format(image['id']))
        except IOError:
            print("Skipping invalid image: {}".format(image['id']))
    return

def get_mongo_collection(db, collection):
    client = MongoClient()
    db = client[db]
    return db[collection]

def search_images(database, collection, image_id):
    collection = get_mongo_collection(database, collection)
    image = collection.find_one({'_id': image_id})
    if image:
        print(image['exif'])
    else:
        print('No image found.')

def index_images(data_url, database, collection, workers):
    startTime = datetime.now()
    mongo_collection = get_mongo_collection(database, collection)
    mongo_collection.drop()
    image_queue = Queue()
    worker_processes = []
    for i in range(workers):
        p = Process(target=image_exif_worker, args=(image_queue, database, collection))
        worker_processes.append(p)
        p.start()
    parse_data(data_url, image_queue)
    for i in range(workers):
        image_queue.put(None)
    for w in worker_processes:
        w.join()
    print('Execution Time: {}'.format(datetime.now() - startTime))

def main():
    parser = argparse.ArgumentParser(description='Waldo Image Application')
    parser.add_argument('-i', '--image-id', type=str, help='Search for image by ID')
    parser.add_argument('-w', '--workers', type=int, default=3, choices=range(1,11), help='Number of worker processes(default=3, max=10)')
    parser.add_argument('-d', '--database', type=str, default='waldo', help='Mongo database name(default=waldo)')
    parser.add_argument('-c', '--collection', type=str, default='images', help='Mongo collection name(default=images)')
    parser.add_argument('-u', '--data-url', type=str, default='http://s3.amazonaws.com/waldo-recruiting/', help='Url containing image data(default=http://s3.amazonaws.com/waldo-recruiting/)')
    kwargs = vars(parser.parse_args())
    if kwargs['image_id']:
        [kwargs.pop(k) for k in ['workers', 'data_url']]
        search_images(**kwargs)
    else:
        kwargs.pop('image_id')
        index_images(**kwargs)

if __name__ == "__main__":
    main()
