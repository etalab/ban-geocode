import csv
import datetime
import os
import sys
import re

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
    'source_id', 'housenumber', 'name', 'postcode', 'city', 'source', 'lat',
    'lon', 'dep', 'region', 'type'
]
DIR = Path(__file__).parent

SYNONYMS = DIR.joinpath('resources', 'synonyms.txt')


def row_to_doc(row):
    dep_id_len = 3 if row['source_id'].startswith('97') else 2
    dep_id = str(row['source_id'])[:dep_id_len]
    context = ', '.join([dep_id, row['dep'], row['region']])
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
        "context": context,
        "type": type_,
    }
    name = row.get('name')
    way_label = None
    way_keywords = None
    if name:
        split = split_address(name)
        if split:
            way_label = split['type']
            way_keywords = split['name']

    if way_label:
        doc['way_label'] = way_label

    housenumber = row.get('housenumber')
    if housenumber:
        els = split_housenumber(housenumber)
        if els:
            doc['housenumber'] = els['number']
            doc['ordinal'] = els['ordinal']
        else:
            doc['housenumber'] = housenumber
        doc['street'] = {'default': name}
        if way_keywords:
            doc['street']['keywords'] = way_keywords
    elif type_ in ['street', 'locality']:
        doc['name'] = {"default": name}
    elif type_ in ['village', 'town', 'city', 'commune']:
        doc['importance'] = 1
        # Sometimes, a village is in reality an hamlet, so it has both a name
        # (the hamlet name) and a city (the administrative entity it belongs
        # to), this is why we first look if a name exists.
        doc['name'] = {'default': name or row.get('city')}
    else:
        doc['name'] = {'default': name}
    if way_keywords and 'name' in doc:
        doc['name']['keywords'] = way_keywords
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


TYPES = [
    'avenue', 'rue', 'boulevard', 'all[ée]es?', 'impasse', 'place',
    'chemin', 'rocade', 'route', 'l[ôo]tissement', 'mont[ée]e', 'c[ôo]te',
    'clos', 'champ', 'bois', 'taillis', 'boucle', 'passage', 'domaine',
    'étang', 'etang', 'quai', 'desserte', 'pré', 'porte', 'square', 'mont',
    'r[ée]sidence', 'parc', 'cours?', 'promenade', 'hameau', 'faubourg',
    'ilot', 'berges?', 'via', 'cit[ée]', 'sent(e|ier)', 'rond[- ][Pp]oint',
    'pas(se)?', 'carrefour', 'traverse', 'giratoire', 'esplanade', 'voie',
]
TYPES_REGEX = '|'.join(
    map(lambda x: '[{}{}]{}'.format(x[0], x[0].upper(), x[1:]), TYPES)
)


def split_address(q):
    m = re.search(
        "^(?P<type>" + TYPES_REGEX + ")"
        "[a-z ']+(?P<name>[\wçàèéuâêôîûöüïäë '\-]+)", q)
    return m.groupdict() if m else {}


def split_housenumber(q):
    m = re.search("^(?P<number>[\d]+)/?(?P<ordinal>([^\d]+|[\d]{1}))?", q)
    return m.groupdict() if m else {}


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
                "index_analyzer": "housenumber_analyzer",
                "copy_to": ["collector"]
            },
            "ordinal": {
                "type": "string",
                "index_analyzer": "housenumber_analyzer",
                "copy_to": ["collector"]
            },
            "way_label": {
                "type": "string",
                "index_analyzer": "raw_stringanalyzer"
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
                        "index": "no",
                        "type": "string",
                        "copy_to": ["collector"],
                    },
                    "alt": {
                        "index": "no",
                        "type": "string",
                        "copy_to": ["collector"]
                    },
                    "keywords": {
                        "type": "string",
                        "index_analyzer": "raw_stringanalyzer",
                    },
                },
            },
            "street": {
                "type": "object",
                "properties": {
                    "default": {
                        "index": "no",
                        "type": "string",
                        "copy_to": ["collector"]
                    },
                    "alt": {
                        "index": "no",
                        "type": "string",
                        "copy_to": ["collector"]
                    },
                    "keywords": {
                        "type": "string",
                        "index_analyzer": "raw_stringanalyzer",
                    }
                }
            },
            "collector": {
                "type": "string",
                "analyzer": "stringanalyzer",
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
            "stringanalyzer": {
                "char_filter": ["punctuationgreedy"],
                "filter": [
                    "word_delimiter", "lowercase", "asciifolding", "synonyms",
                    "banolength", "unique", "wordending", "banongram"
                ],
                "tokenizer": "standard"
            },
            "search_stringanalyzer": {
                "char_filter": ["punctuationgreedy"],
                "filter": [
                    "word_delimiter", "lowercase", "asciifolding", "synonyms",
                    "banolength", "unique", "wordendingautocomplete"
                ],
                "tokenizer": "standard"
            },
            "raw_stringanalyzer": {
                "char_filter": ["punctuationgreedy"],
                "filter": [
                    "word_delimiter", "lowercase", "asciifolding", "synonyms",
                    "banolength", "unique", "wordending"
                ],
                "tokenizer": "standard"
            },
            "housenumber_analyzer": {
                "char_filter": ["punctuationgreedy"],
                "filter": [
                    "word_delimiter", "lowercase", "asciifolding", "wordending"
                ],
                "tokenizer": "standard"
            }
        },
        "filter": {
            "banongram": {
                "min_gram": "2",
                "type": "edgeNGram",
                "max_gram": "20"  # Scharrachbergheim => 17
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
