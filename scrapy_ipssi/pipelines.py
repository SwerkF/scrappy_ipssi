
# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import pymongo
import os
from dotenv import load_dotenv

load_dotenv()

class ScrapyIpssiPipeline:
    def __init__(self):
        mongo_user = os.getenv("MONGODB_USERNAME", "root")
        mongo_password = os.getenv("MONGODB_PASSWORD", "password") 
        mongo_host = os.getenv("MONGODB_URL", "localhost:27017")
        mongo_db = os.getenv("MONGODB_DATABASE", "kbo")
        
        connection_string = f"mongodb://{mongo_user}:{mongo_password}@{mongo_host}/"
        self.client = pymongo.MongoClient(connection_string)
        self.db = self.client[mongo_db]
        self.collection = self.db["entreprises"]

    def process_item(self, item, spider):
        self.collection.insert_one(dict(item))
        return item
