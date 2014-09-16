# BANO for Elasticsearch

This is a set of tools to index and search BANO data in Elasticsearch.

## Install

You need python3.4 and Elasticsearch >= 1.3.

For Elasticsearch, grab the `.deb` from http://www.elasticsearch.org/downloads/,
then:

    `sudo dpkg -i <path/to/elasticsearch.deb>`

You need the `wordending-tokenfilter` ES plugin:

    ```
    git clone https://github.com/ixxi-mobility/elasticsearch-wordending-tokenfilter.git
    cd elasticsearch-wordending-tokenfilter
    make package
    make install
    ```


Step to install the python environment:

1. make sure you have virtualenv and virtualenv-wrapper

    `sudo apt-get install python-virtualenv virtualenv-wrapper`

1. create your virtualenv (named bano here, your can change that to whatever)

    `mkvirtualenv bano --python `which python3.4``

1. get the project

    ```
    git clone https://github.com/yohanboniface/bano-geocode.git
    cd bano-geocode
    ```

1. install dependencies

    `pip install -r requirements.txt`

1. get BANO data (or any subset from http://bano.openstreetmap.fr/data/)

    ```
    wget http://bano.openstreetmap.fr/data/full.csv.bz2
    bunzip2 full.csv.bz2
    ```

1. import data into ES (it should take around 10 minutes):

    `python run.py import full.csv`

1. run the API lite server

    `python run.py serve`

1. start searching

    `curl 'http://localhost:5005/api/?q=5 rue Guersant'`


To get more config options:

    `python run.py --help`
