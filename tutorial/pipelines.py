# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import time
from itemadapter import ItemAdapter
import logging
from tutorial.settings import SingleMONGODB_SERVER, SingleMONGODB_PORT
from pymongo import MongoClient
import traceback

class SpiderPipeline:
    def __init__(self):
        self.mongodb_server = SingleMONGODB_SERVER
        self.mongodb_port = SingleMONGODB_PORT
        self.mongodb_db = "electron_spider"
        try:
            self.client = MongoClient(host=self.mongodb_server, port=self.mongodb_port)
            self.db = self.client[self.mongodb_db]
        except Exception as e:
            traceback.print_exc()
    def process_item(self, item, spider):
        self.saveData(item, item.get('collection'))
        return item

    def saveData(self, item, collection):
        query = {}
        for field, value in item.items():
            if field != "timeStamp":
                query[field] = item.get(field, " ")
        self.db[collection].update_one(query, {"$set": item}, upsert=True)
