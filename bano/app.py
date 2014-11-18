import json
import os
import csv
import io
import logging
import re

import elasticsearch
from elasticsearch_dsl import Search, Q
from elasticsearch_dsl.filter import F
from elasticsearch_dsl.query import Match, Filtered
from flask import Flask, render_template, request, abort, Response


app = Flask(__name__)
PORT = os.environ.get('BANO_PORT', 5001)
HOST = os.environ.get('BANO_HOST', '0.0.0.0')
API_URL = os.environ.get('API_URL', '/search/?')
CENTER = [
    float(os.environ.get('BANO_MAP_LAT', 48.7833)),
    float(os.environ.get('BANO_MAP_LON', 2.2220))
]
TILELAYER = os.environ.get(
    'BANO_MAP_TILELAYER',
    'http://{s}.tile.openstreetmap.fr/osmfr/{z}/{x}/{y}.png'
)
MAXZOOM = os.environ.get('BANO_MAP_MAXZOOM', 19)
INDEX = os.environ.get('BANO_INDEX', 'bano')

es = elasticsearch.Elasticsearch(timeout=999999999)


class NotFoundLogHandler(logging.FileHandler):

    def __init__(filename, *args, **kwargs):
        super().__init__('notfound.log', *args, **kwargs)


notfound = logging.getLogger('notfound')
notfound.setLevel(logging.DEBUG)
notfound.addHandler(NotFoundLogHandler())


@app.route('/')
def index():
    return render_template(
        'index.html',
        API_URL=API_URL,
        CENTER=CENTER,
        TILELAYER=TILELAYER,
        MAXZOOM=MAXZOOM
    )


def preprocess(q):
    q = re.sub('(Cedex|Cédex) ?[\d]*', '', q, flags=re.IGNORECASE)
    q = re.sub('bp ?[\d]*', '', q, flags=re.IGNORECASE)
    q = re.sub('cs ?[\d]*', '', q, flags=re.IGNORECASE)
    q = re.sub(' {2,}', ' ', q, flags=re.IGNORECASE)
    q = q.strip()
    return q


def match_address(q):
    m = re.search('([\d]*(,? )?(avenue|rue|boulevard|allées?|impasse|place) .*([\d]{5})?).*', q, flags=re.IGNORECASE)
    if m:
        return m.group()


def make_query(q, lon=None, lat=None, match_all=True, limit=15, filters=None):
    if filters is None:
        filters = {}
    s = Search(es).index(INDEX)
    should_match = '100%' if match_all else '2<-1 6<-2 8<-3 10<-50%'
    # if not match_all:
    #     q = preprocess(q)
    match = Q(
        'bool',
        must=[Q('match', collector={
            'fuzziness': 1,
            'prefix_length': 2,
            'query': q,
            'minimum_should_match': should_match,
            'analyzer': 'search_stringanalyzer'
        })],
        should=[
            Q('match', **{'name.keywords': {
                'query': q,
                'boost': 2,
                'analyzer': 'search_stringanalyzer'
            }}),
            Q('match', **{'street.keywords': {
                'query': q,
                'boost': 2,
                'analyzer': 'search_stringanalyzer'
            }}),
            Q('match', **{'city.default': {
                'query': q,
                'boost': 2,
                'analyzer': 'search_stringanalyzer'
            }}),
            Q('match', **{'way_label': {
                'query': q,
                'boost': 2,
                'analyzer': 'search_stringanalyzer'
            }}),
            Q('match', **{'housenumber': {
                'query': q,
                'boost': 2,
                'analyzer': 'housenumber_analyzer'
            }}),
        ]
    )

    functions = [{
        "script_score": {
            "script": "1 + doc['importance'].value * 40",
            "lang": "groovy"
        }
    }]
    if lon is not None and lat is not None:
        functions.append({
            "script_score": {
                "script": "dist = doc['coordinate'].distanceInKm(lat, lon); 1 / (0.5 - 0.5 * exp(-5*dist/maxDist))",
                "lang": "groovy",
                "params": {
                    "lon": lon,
                    "lat": lat,
                    "maxDist": 100
                }
            }
        })

    fscore = Q(
        'function_score',
        score_mode="multiply",
        boost_mode="multiply",
        query=match,
        functions=functions
    )

    s = s.query(fscore)
    # Only filter out 'house' if we are not explicitly asking for this
    # type.
    if filters.get('type') is not 'housenumber':
        # We don't want results with an ordinal (bis, ter…) if the ordinal
        # field itself doesn't match
        filter_ordinal = F('or', [
            F('missing', field="ordinal"),
            F({"query": {"match": {"ordinal": {"query": q, "analyzer": "housenumber_analyzer"}}}}),
        ])
        house_query = Filtered(query=Match(housenumber={"query": q, "analyzer": "housenumber_analyzer"}), filter=filter_ordinal)
        filter_house = F('or', [
            F('missing', field="housenumber"),
            F('exists', field="name.keywords"),
            F({'query': house_query.to_dict()}),
        ])
        s = s.filter(filter_house)
    if filters:
        # We are not using real filters here, because filters are not analyzed,
        # so for example "city=Chauny" will not match, because "chauny" is in
        # the index instead.
        for k, v in filters.items():
            s = s.query({'match': {k: v}})
    return s.extra(size=limit)


def query_index(q, lon, lat, match_all=True, limit=15, filters=None):
    s = make_query(q, lon, lat, match_all, limit, filters)
    if app.debug:
        print(json.dumps(s.to_dict()))
    return s.execute()


@app.route('/search/')
def search():

    try:
        lon = float(request.args.get('lon'))
        lat = float(request.args.get('lat'))
    except (TypeError, ValueError):
        lon = lat = None

    try:
        limit = min(int(request.args.get('limit')), 50)
    except (TypeError, ValueError):
        limit = 15

    query = request.args.get('q')
    if not query:
        abort(400, "missing search term 'q': /?q=berlin")

    filters = {}
    keys = ['type', 'city', 'postcode', 'housenumber', 'street']
    nested = ['street', 'city']
    for key in keys:
        value = request.args.get(key)
        if value:
            if key in nested:
                key = '{}.default'.format(key)
            filters[key] = value

    results = query_index(query, lon, lat, limit=limit, filters=filters)

    if len(results.hits) < 1:
        query = preprocess(query)
        results = query_index(query, lon, lat,
                              match_all=True, limit=limit, filters=filters)

    if len(results.hits) < 1:
        # Try without any number.
        no_num = re.sub('[\d]*', '', query)
        results = query_index(no_num, lon, lat,
                              match_all=True, limit=limit, filters=filters)

    if len(results.hits) < 1:
        # Try matching a standard address pattern.
        match = match_address(query)
        if match:
            results = query_index(match, lon, lat,
                                  match_all=False, limit=limit, filters=filters)

    if len(results.hits) < 1:
        # No result could be found, query index again and don't expect to match
        # all search terms.
        results = query_index(query, lon, lat,
                              match_all=False, limit=limit, filters=filters)

    if len(results.hits) < 1:
        notfound.debug(query)

    debug = 'debug' in request.args
    data = to_geo_json(results, debug=debug)
    data = json.dumps(data, indent=4 if debug else None)
    response = Response(data, mimetype='application/json')
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "X-Requested-With"
    return response


@app.route('/multisearch/', methods=['GET', 'POST'])
def multi_search():
    if request.method == 'POST':
        f = request.files['data']
        first_line = next(f.stream).decode().strip('\n')
        dialect = csv.Sniffer().sniff(first_line)
        headers = first_line.split(dialect.delimiter)
        columns = request.form.getlist('columns') or headers
        match_all = is_bool(request.form.get('match_all'))
        content = f.read().decode().split('\n')
        rows = csv.DictReader(content, fieldnames=headers, dialect=dialect)
        search = []
        queries = []
        for row in rows:
            q = ' '.join({k: row[k] for k in columns}.values())
            queries.append(q)
            query = make_query(q, limit=1, match_all=match_all)
            search.append({'index': 'bano'})
            search.append(query.to_dict())
        responses = []
        start = 0
        step = 200
        while start <= len(search):
            chunk = search[start:start + step]
            start += step
            responses.extend(es.msearch(chunk)['responses'])
        fieldnames = headers
        fieldnames.extend(['latitude', 'longitude', 'address'])
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames, dialect=dialect)
        writer.writeheader()
        rows = csv.DictReader(content, fieldnames=headers, dialect=dialect)
        for row, response, q in zip(rows, responses, queries):
            if not 'error' in response:
                if response['hits']['total']:
                    try:
                        source = response['hits']['hits'][0]['_source']
                    except IndexError:
                        # Yes, we can have a total > 0 AND no hits :/
                        pass
                    else:
                        row.update({
                            'latitude': source['coordinate']['lat'],
                            'longitude': source['coordinate']['lon'],
                            'address': to_flat_address(source),
                        })
                else:
                    notfound.debug(q)
            writer.writerow(row)
        output.seek(0)
        headers = {
            'Content-Disposition': 'attachment',
            'Content-Type': 'text/csv'
        }
        return output.read(), 200, headers
    if 'text/html' in request.headers['Accept']:
        return render_template('multisearch.html')


def to_geo_json(hits, debug=False):
    features = []
    for hit in hits:

        properties = {}

        flat_keys = [
            'osm_key', 'osm_value', 'postcode', 'housenumber', 'type',
            'context', 'ordinal'
        ]
        for attr in flat_keys:
            if hasattr(hit, attr):
                properties[attr] = hit[attr]

        for attr in ['name', 'city', 'street']:
            obj = hit.get(attr, {})
            value = obj.get('default')
            if value:
                properties[attr] = value

        if not 'name' in properties and 'housenumber' in properties:
            housenumber = properties.get('housenumber', '')
            ordinal = properties.get('ordinal', '')
            street = properties.get('street', '')
            els = [housenumber, ordinal, street]

            properties['name'] = ' '.join([el for el in els if el])

        feature = {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [
                    hit['coordinate']['lon'],
                    hit['coordinate']['lat']
                ]
            },
            "properties": properties
        }

        features.append(feature)

    return {
        "type": "FeatureCollection",
        "features": features
    }


def to_flat_address(hit):
    els = [
        hit.get('housenumber', ''),
        hit.get('street', {}).get('default', ''),
        hit.get('name', {}).get('default', ''),
        hit.get('postcode', ''),
        hit.get('city', {}).get('default', ''),
    ]
    return " ".join([e for e in els if e])


def is_bool(what):
    what = str(what).lower()
    return what in ['true', '1']
