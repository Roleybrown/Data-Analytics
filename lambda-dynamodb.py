import requests
import json
import boto3
from decimal import Decimal


def lambda_handler(event, context):
   
    dynamo_db = boto3.resource('dynamodb')
    table = dynamo_db.Table('Cape-Town-Health-Facilities')

    url = 'https://citymaps.capetown.gov.za/agsext1/rest/services/Theme_Based/Open_Data_Service/MapServer/44/query?where=1%3D1&outFields=*&outSR=4326&f=json'

    response = requests.get(url)

    response_json = json.loads(response.text)
    
    facilities = json.loads(json.dumps(response_json), parse_float=Decimal)

    with table.batch_writer() as batch:
    
        for facility in facilities["features"]:
            
            facilityid = facility["attributes"]["OBJECTID"]
            facilityname = facility["attributes"]["NAME"]
            facilityaddress = facility["attributes"]["ADR"]
            longitude = facility["geometry"]["x"]
            latitude = facility["geometry"]["y"]
            
            batch.put_item(
                Item = {
                    'facilityid': facilityid,
                    'facilityname': facilityname,
                    'facilityaddress': facilityaddress,
                    'longitude': longitude,
                    'latitude': latitude
                    }
                
                )