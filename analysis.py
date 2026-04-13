from pymongo import MongoClient
from econ_api import *
from dotenv import load_dotenv

load_dotenv()

# load in values from .env file
client = MongoClient(os.getenv('MONGO_URI'))
db = client[os.getenv('DB_NAME')]
yearly_trends = db[os.getenv('COLLECTION1')]
countries = db.countries

#initialize client
client = MongoClient('mongodb://localhost:27017/')
db = client['ted_global']
yearly_trends = db.yearly_trends
regions = db.regions
countries = db.countries

import_csv('ted_data.csv')

countries_info = [
    {

    }
]