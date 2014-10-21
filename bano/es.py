import csv
import datetime
import os
import sys
from pathlib import Path

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk_index


ES = Elasticsearch()


def timestamp_index(index):
    return '%s-%s' % (index, datetime.datetime.now().strftime('%Y%m%d%H%M%S'))


def create_index(index):
    # ES.indices.delete(index, ignore=404)
    index = timestamp_index(index)

    ES.indices.create(
        index,
        body={'mappings': MAPPINGS, 'settings': {'index': SETTINGS}}
    )
    print('index created:', index)
    return index


def update_aliases(alias, index):
    olds = ES.indices.get_aliases(alias, ignore=404)
    actions = []
    for old in olds:
        actions.append({"remove": {'index': old, 'alias': alias}})
    actions.append({"add": {'index': index, 'alias': alias}})
    print('Running update_aliases actions', actions)
    ES.indices.update_aliases({'actions': actions}, ignore=404)


DUMPPATH = os.environ.get('BANO_DUMPPATH', '/tmp')
FIELDS = [
    'source_id', 'housenumber', 'street', 'postcode', 'city', 'source', 'lat',
    'lon', 'dep', 'region', 'type'
]
DIR = Path(__file__).parent

SYNONYMS = DIR.joinpath('resources', 'synonyms.txt')


def row_to_doc(row):
    context = ', '.join([row['dep'], row['region']])
    # type can be:
    # - number => housenumber
    # - street => street
    # - hamlet => locality found in OSM as place=hamlet
    # - place => locality not found in OSM
    # - village => village
    # - town => town
    # - city => city
    type_ = row['type']
    if type_ == 'number':
        type_ = 'housenumber'
    elif type_ in ['hamlet', 'place']:
        type_ = 'locality'
    doc = {
        "importance": 0.0,
        "coordinate": {
            "lat": row['lat'],
            "lon": row['lon']
        },
        "postcode": row['postcode'],
        "city": {
            "default": row['city'],
        },
        "country": "France",
        "street": {
            "default": row['street'],
        },
        "context": context,
        "type": type_,
    }
    if type_ == 'housenumber':
        doc['housenumber'] = row['housenumber']
    elif type_ in ['street', 'locality']:
        doc['name'] = {"default": row['street']}
    elif type_ in ['village', 'town', 'city']:
        doc['importance'] = 1
        doc['name'] = {"default": row['city']}
    return doc


def bulk(index, data):
    bulk_index(ES, data, index=index, doc_type="place", refresh=True)


def import_data(index, filepath, limit=None):
    print('Importing from', filepath)
    with open(filepath) as f:
        reader = csv.DictReader(f, fieldnames=FIELDS, delimiter='|')
        count = 0
        data = []
        for row in reader:
            data.append(row_to_doc(row))
            count += 1
            if limit and count >= limit:
                break
            if count % 100000 == 0:
                bulk(index, data)
                data = []
                sys.stdout.write("Done {}\n".format(count))
        if data:
            bulk(index, data)
            sys.stdout.write("Done {}\n".format(count))


MAPPINGS = {
    "place": {
        "dynamic": "false",
        "_all": {"enabled": False},
        "_id": {"path": "id"},
        "properties": {
            "type": {"type": "string"},
            "importance": {"type": "float"},
            "housenumber": {
                "type": "string",
                "index_analyzer": "raw_stringanalyser",
                "copy_to": ["collector"]
            },
            "coordinate": {"type": "geo_point"},
            "postcode": {
                "type": "string",
                "index": "not_analyzed",
                "copy_to": ["collector"]
            },
            "city": {
                "type": "object",
                "properties": {
                    "alt": {
                        "type": "string",
                        "copy_to": ["collector"]
                    },
                    "default": {
                        "type": "string",
                        "copy_to": ["collector"]
                    },
                }
            },
            "context": {
                "type": "string",
                "index": "no",
                "copy_to": ["collector"]
            },
            "country": {
                "type": "string",
                "index": "no",
                "copy_to": ["collector"]
            },
            "name": {
                "type": "object",
                "properties": {
                    "default": {
                        "type": "string",
                        "index_analyzer": "raw_stringanalyser",
                        "copy_to": ["collector"],
                    },
                    "alt": {
                        "type": "string",
                        "index_analyzer": "raw_stringanalyser",
                        "copy_to": ["collector"]
                    },
                },
            },
            "street": {
                "type": "object",
                "properties": {
                    "default": {
                        "type": "string",
                        "copy_to": ["collector"]
                    },
                    "alt": {
                        "index": "no",
                        "type": "string",
                        "copy_to": ["collector"]
                    }
                }
            },
            "collector": {
                "type": "string",
                "analyzer": "stringanalyser",
                "fields": {
                    "raw": {
                        "type": "string",
                        "analyzer": "raw_stringanalyser"
                    }
                }
            },
        }
    }
}


SETTINGS = {
    "analysis": {
        "char_filter": {
            "punctuationgreedy": {
                "type": "pattern_replace",
                "pattern": "[\\.,]"
            }
        },
        "analyzer": {
            "stringanalyser": {
                "char_filter": ["punctuationgreedy"],
                "filter": [
                    "word_delimiter", "lowercase", "asciifolding", "synonyms",
                    "banolength", "unique", "wordending", "banongram"
                ],
                "tokenizer": "standard"
            },
            "search_stringanalyser": {
                "char_filter": ["punctuationgreedy"],
                "filter": [
                    "word_delimiter", "lowercase", "asciifolding", "synonyms",
                    "banolength", "unique", "wordendingautocomplete"
                ],
                "tokenizer": "standard"
            },
            "raw_stringanalyser": {
                "char_filter": ["punctuationgreedy"],
                "filter": [
                    "word_delimiter", "lowercase", "asciifolding", "synonyms",
                    "banolength", "unique", "wordending"
                ],
                "tokenizer": "standard"
            }
        },
        "filter": {
            "banongram": {
                "min_gram": "2",
                "type": "edgeNGram",
                "max_gram": "15"
            },
            "wordending": {
                "type": "wordending",
                "mode": "default"
            },
            "banolength": {
                "min": "2",
                "type": "length"
            },
            "wordendingautocomplete": {
                "type": "wordending",
                "mode": "autocomplete"
            },
            "synonyms": {
                "type": "synonym",
                "synonyms_path": str(SYNONYMS)
            },
        }
    }
}
