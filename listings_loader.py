import argparse
import csv
import json

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import TransportError
from elasticsearch.helpers import bulk


def generate_listings():
    index_name = 'listings'

    with open('mappings/listings.json') as mapping:
        es.indices.create(
            index=index_name,
            body=json.load(mapping),
        )

    listings = {}

    with open('data/listingsFull.csv') as listings_data:
        reader = csv.DictReader(listings_data)
        for row_data in reader:
            listings[row_data['id']] = row_data

    all_data = []
    with open('data/listings.csv') as csvfile:
        reader = csv.DictReader(csvfile)
        for row_data in reader:
            latitude = row_data.pop('latitude')
            longitude = row_data.pop('longitude')

            row_data['location'] = {
                'lat': latitude,
                'lon': longitude,
            }

            if row_data.get('last_review') == '':
                row_data.pop('last_review')
            
            listing_data = listings[row_data['id']]

            row_data['city'] = listing_data['city']
            row_data['property_type'] = listing_data['property_type']
            row_data['accomodates'] = listing_data['accommodates']
            row_data['summary'] = listing_data['summary']
            review_score = listing_data.get('review_scores_rating')
            if review_score:
                row_data['listing_review_score'] = int(review_score)
                row_data['listing_review_score_normalized'] = int(int(review_score)/10)

            all_data.append({
                "_index": index_name,
                "_type": "listing",
                "_source": row_data
            })
        
        return all_data

if __name__ == '__main__':   
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-H", "--host",
        action="store",
        default="localhost:9200",
        help="The elasticsearch host you wish to connect to. (Default: localhost:9200)")
    
    args = parser.parse_args()
    es = Elasticsearch([args.host])
    bulk(es, generate_listings())