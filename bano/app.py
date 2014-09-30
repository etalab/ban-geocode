import json
import os

import elasticsearch
from elasticsearch_dsl import Search, Q, query
from elasticsearch_dsl.filter import F
from flask import Flask, render_template, request, abort, Response


app = Flask(__name__)
DEBUG = os.environ.get('DEBUG', True)
PORT = os.environ.get('BANO_PORT', 5001)
HOST = os.environ.get('BANO_HOST', '0.0.0.0')

es = elasticsearch.Elasticsearch()


@app.route('/')
def index():
    return render_template('index.html')


def query_index(q, lon, lat, match_all=True, limit=15):
    s = Search(es)
    multi = query.MultiMatch(
        query=q,
        type="best_fields",
        analyzer="search_stringanalyser",
        minimum_should_match='100%' if match_all else -1,
        fields=[
            "name.default^3",
            "collector"
        ],
        fuzziness=1,
        prefix_length=3
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
        query=multi,
        functions=functions
    )

    s = s.query(fscore)
    filters = F('or', [
        F('missing', field="housenumber"),
        F({"query": {"match": {"housenumber": {"query": q, "analyzer": "standard"}}}}),
        F('exists', field="name.default")
    ])
    s = s.filter(filters)
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

    results = query_index(query, lon, lat, limit=limit)

    if len(results.hits) < 1:
        # No result could be found, query index again and don't expect to match
        # all search terms.
        results = query_index(query, lon, lat, match_all=False, limit=limit)

    debug = 'debug' in request.args
    data = to_geo_json(results, debug=debug)
    data = json.dumps(data, indent=4 if debug else None)
    return Response(data, mimetype='application/json')


def housenumber_first(lang):
    if lang in ['de', 'it']:
        return False

    return True


def to_geo_json(hits, lang='en', debug=False):
    features = []
    for hit in hits:

        properties = {}

        for attr in ['osm_key', 'osm_value', 'postcode', 'housenumber']:
            if hasattr(hit, attr):
                properties[attr] = hit[attr]

        # language specific mapping
        for attr in ['name', 'city', 'street']:
            obj = hit.get(attr, {})
            value = obj.get(lang) or obj.get('default')
            if value:
                properties[attr] = value

        if not 'name' in properties and 'housenumber' in properties:
            housenumber = properties['housenumber'] or ''
            street = properties['street'] or ''

            if housenumber_first(lang):
                properties['name'] = housenumber + ' ' + street
            else:
                properties['name'] = street + ' ' + housenumber

        feature = {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [hit['coordinate']['lon'], hit['coordinate']['lat']]
            },
            "properties": properties
        }

        features.append(feature)

    return {
        "type": "FeatureCollection",
        "features": features
    }
