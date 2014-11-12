#!/usr/bin/env python
"""
Ixxi lib for importing various data into ElasticSearch.
Usage:
    run.py serve [--port=<number>] [--host=<string>] [options]
    run.py import <filepath>... [--index=<string>] [options]

Examples:
    python run.py serve --port=5050
    python run.py import full.csv

Options:
    -h --help           print this message and exit
    --port=<number>     server port [default: 5005]
    --host=<string>     server host [default: 127.0.0.1]
    --index=<string>    index name to use in elasticsearch [default: bano]
    --debug             turn on debug mode [default: False]
    --limit=<number>    add a limit when it makes sense [default: 0]
"""
import os

from docopt import docopt

from bano.es import create_index, import_data, update_aliases
from bano.app import app


if __name__ == '__main__':
    args = docopt(__doc__, version='Bano Search 0.1')
    app.debug = args['--debug'] or os.environ.get('DEBUG', False)
    if args['serve']:
        app.run(port=int(args['--port']), host=args['--host'])
    elif args['import']:
        name = create_index(args['--index'])
        if args['--limit']:
            limit = int(args['--limit'])
        for filepath in args['<filepath>']:
            import_data(name, filepath, limit=limit)
        update_aliases(args['--index'], name)
