from pymongo import MongoClient
from dotenv import load_dotenv
import os


load_dotenv()
DB_URL = os.getenv("DB_URL")
DB_COLLECTION = os.getenv("DB_COLLECTION")


def config(collectionName):
    db = MongoClient(DB_URL)[DB_COLLECTION]
    collection = db[collectionName]
    return collection
