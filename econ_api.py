import re
import pandas as pd
from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client['ted_global']
yearly_trends = db.yearly_trends

#initialize csv data into database
def import_csv(csv):
    client.drop_database('ted_global')

    df = pd.read_csv(csv)
    yearly_trends.insert_many(df.to_dict(orient='records'))

def mongo_filter(filters):
    """
    Initialize filters for Mongo syntax.

    Args:
        filters: list of tuples of filters as string that the user wants to ask, allows >, <, >=, <=, ==, and contains
        e.g. [('artist', 'Kendrick Lamar'), ('energy', ('>', 50)), ('song', ('contains', 'love'))]

    Returns: filter as a dictionary in Mongo syntax.
    """
    operator_map = {'>': '$gt', '>=': '$gte', '<': '$lt', '<=': '$lte', '==': '$eq', 'contains': '$regex'}
    output_filter = {}
    if not isinstance(filters, list):
        filters = [filters]

    for field, condition in filters:
        # case 1: simple equality
        if not isinstance(condition, tuple):
            output_filter[field] = condition
        else:
            # condition is a tuple like ('>', 70) or ('contains', 'Love')
            op, val= condition
            if op == 'contains':
                output_filter[field] = re.compile(val, re.IGNORECASE)
            else:
                output_filter[field] = {operator_map[op]: val}
    return output_filter

def find_songs(filters=None, fields=None, sort=None, num_songs=5):
    """
    Find a song/songs by filter, output fields, sorts and number of songs.
    Args:
        filters: filters for querying in Python, for format see mongo_filter above
        fields: fields to include in output, for format see define_outputs above
        sort (list of tuples): sorting order, e.g., [('popularity', -1), ('energy', -1)]
    """
    if filters:
        filters = mongo_filter(filters)
    else:
        filters = {}

    if fields:
        outputs = define_outputs(fields)
    else:
        outputs = {'_id': 0}

    songs = db.songs.find(filters, outputs)

    if sort:
        if not isinstance(sort, list):
            sort = [sort]
        songs = songs.sort(sort)

    songs = songs.limit(num_songs)

    for s in songs:
        print(s)

def aggregate_songs(matches=None, group_by=None, metrics=None, sort=None, fields=None, num_songs=5):
    """
    Find songs using MongoDB aggregation.

    Parameters:
        matches: matches to apply before aggregation, for format see mongo_filter above
        group_by: field(s) to group by, e.g., 'artist' or ['artist', 'genre']
        metrics (tuple): metrics to compute, e.g., [('avg', 'energy'), ('max', 'popularity')]
        sort: sorting order after aggregation, e.g., [('avg_energy', -1)]
        num_songs (int): number of results to return
    """
    agg = []

    if matches:
        agg.append({'$match': mongo_filter(matches)})


    if group_by:
        if isinstance(group_by, list):
            groups = {'_id': {field: f'${field}' for field in group_by}}
        else:
            groups = {'_id': f'${group_by}'}

        if metrics:
            if not isinstance(metrics, list):
                metrics = [metrics]

            for metric in metrics:
                agg_type, agg_metric = metric
                groups[f'{agg_type}_{agg_metric}'] = {f'${agg_type}': f'${agg_metric}'}

        agg.append({'$group': groups})

    if sort:
        if isinstance(sort, list):
            sort = dict(sort)
        agg.append({'$sort': sort})

    if fields:
        projection = define_outputs(fields)
        agg.append({'$project': projection})

    agg.append({'$limit': num_songs})

    songs = db.songs.aggregate(agg)
    for s in songs:
        print(s)