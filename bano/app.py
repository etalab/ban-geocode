import json
import os
import csv
import io

import elasticsearch
from elasticsearch_dsl import Search, Q
from elasticsearch_dsl.filter import F
from flask import Flask, render_template, request, abort, Response


app = Flask(__name__)
DEBUG = os.environ.get('DEBUG', True)
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


@app.route('/')
def index():
    return render_template(
        'index.html',
        API_URL=API_URL,
        CENTER=CENTER,
        TILELAYER=TILELAYER,
        MAXZOOM=MAXZOOM
    )


def make_query(q, lon=None, lat=None, match_all=True, limit=15, filters=None):
    if filters is None:
        filters = {}
    s = Search(es).index(INDEX)
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
    return s


def query_index(q, lon, lat, match_all=True, limit=15, filters=None):
    s = make_query(q, lon, lat, match_all, limit, filters)
    if DEBUG:
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


@app.route('/multisearch/', methods=['GET', 'POST'])
def multi_search():
    if request.method == 'POST':
        f = request.files['data']
        dialect = csv.Sniffer().sniff(f.read(1024).decode())
        f.seek(0)
        headers = next(f.stream).decode().strip('\n').split(dialect.delimiter)
        columns = request.form.getlist('columns') or headers
        match_all = is_bool(request.form.get('match_all'))
        content = f.read().decode().split('\n')
        rows = csv.DictReader(content, fieldnames=headers, dialect=dialect)
        search = []
        for row in rows:
            q = ' '.join({k: row[k] for k in columns}.values())
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
        for row, response in zip(rows, responses):
            if not 'error' in response and response['hits']['total']:
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
            street = properties.get('street', '')

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
