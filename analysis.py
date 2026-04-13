from pymongo import MongoClient
from econ_api import *

#initialize client
client = MongoClient('mongodb://localhost:27017/')
db = client['ted_global']
yearly_trends = db.yearly_trends
regions = db.regions
countries = db.countries

import_csv('ted_data.csv')
region_info = [
    {
      "_id": "ASIA",
      "name": "Asia",
      "countries": ["JPN", "TWN", "CHN"]
    },
    {
      "_id": "EURO",
      "name": "Europe",
      "countries": ["DEU", "SWE", "ITA"]
    },
    {
      "_id": "AMER",
      "name": "Americas",
      "countries": ["USA", "CAN", "MEX"]
    }
]
regions.insert_many(region_info)

countries_info = [
    {

    }
]