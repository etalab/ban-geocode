import json
import os

import elasticsearch
from elasticsearch_dsl import Search, Q
from elasticsearch_dsl.filter import F
from flask import Flask, render_template, request, abort, Response


app = Flask(__name__)
DEBUG = os.environ.get('DEBUG', True)
PORT = os.environ.get('BANO_PORT', 5001)
HOST = os.environ.get('BANO_HOST', '0.0.0.0')
API_URL = os.environ.get('API_URL', '/api/?')
CENTER = [
    float(os.environ.get('BANO_MAP_LAT', 48.7833)),
    float(os.environ.get('BANO_MAP_LON', 2.2220))
]
TILELAYER = os.environ.get(
    'BANO_MAP_TILELAYER',
    'http://{s}.tile.openstreetmap.fr/osmfr/{z}/{x}/{y}.png'
)
MAXZOOM = os.environ.get('BANO_MAP_MAXZOOM', 19)

es = elasticsearch.Elasticsearch()


@app.route('/')
def index():
    return render_template(
        'index.html',
        API_URL=API_URL,
        CENTER=CENTER,
        TILELAYER=TILELAYER,
        MAXZOOM=MAXZOOM
    )


def query_index(q, lon, lat, match_all=True, limit=15, filters=None):
    if filters is None:
        filters = {}
    s = Search(es)
    should_match = '100%' if match_all else -1
    match = Q(
        'bool',
        must=[Q('match', collector={
            'fuzziness': 1,
            'prefix_length': 2,
            'query': q,
            'minimum_should_match': should_match,
            'analyzer': 'search_stringanalyser'
        })],
        should=[
            Q('match', **{'name.default': {
                'query': q,
                'boost': 200
            }}),
            Q('match', **{'street.default': {
                'query': q,
                'boost': 100
            }}),
            Q('match', **{'city.default': {
                'query': q,
                'boost': 50
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
    if not filters.get('type') == 'housenumber':
        # Only filter out 'house' if we are not explicitly asking for this
        # type.
        filter_house = F('or', [
            F('missing', field="housenumber"),
            F({"query": {"match": {"housenumber": {"query": q, "analyzer": "raw_stringanalyser"}}}}),
            F('exists', field="name.default")
        ])
    s = s.filter(filter_house)
    if filters:
        # We are not using real filters here, because filters are not analyzed,
        # so for example "city=Chauny" will not match, because "chauny" is in
        # the index instead.
        for k, v in filters.items():
            s = s.query({'match': {k: v}})
    if DEBUG:
        print(json.dumps(s.to_dict()))
    return s.execute()


@app.route('/api/')
def api():

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
        # No result could be found, query index again and don't expect to match
        # all search terms.
        results = query_index(query, lon, lat,
                              match_all=False, limit=limit, filters=filters)

    debug = 'debug' in request.args
    data = to_geo_json(results, debug=debug)
    data = json.dumps(data, indent=4 if debug else None)
    response = Response(data, mimetype='application/json')
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "X-Requested-With"
    return response


def to_geo_json(hits, debug=False):
    features = []
    for hit in hits:

        properties = {}

        flat_keys = [
            'osm_key', 'osm_value', 'postcode', 'housenumber', 'type',
            'context'
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
            housenumber = properties['housenumber'] or ''
            street = properties['street'] or ''

            properties['name'] = ' '.join([housenumber, street])

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
