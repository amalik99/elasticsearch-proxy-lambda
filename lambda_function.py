import base64
import os
import requests
import logging
import json
import gzip

HOST = os.environ.get('ELASTICSEARCH_HOST')
API_KEY = os.environ.get('ELASTICSEARCH_API_KEY')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def format_response(response):
    resp_body = response.content
    headers = dict(response.headers)

    # Convert binary data to base64-encoded string
    resp_body = base64.b64encode(resp_body).decode()

    # Remove 'Content-Encoding' header
    headers.pop('Content-Encoding', None)

    logger.info(f"Response body: {resp_body}")
    logger.info(f"Response headers: {headers}")

    return (headers, resp_body, True)


def lambda_handler(event, context):
    logger.info('Event: ' + json.dumps(event))
    method = event['requestContext']['http']['method']
    path = event['requestContext']['http']['path']
    
    if not method:
        logger.error('No HTTP method found in the event')
        # Handle the error appropriately, e.g., return an error response
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'No HTTP method specified'})
        }
    body = event.get('body')
    if event.get('isBase64Encoded', False):
        body = base64.b64decode(body)
    query_params = event.get('queryStringParameters', {})
    headers = event.get('headers', {})

    req_headers = {'Authorization': f'ApiKey {API_KEY}'}

    # Add necessary headers from the incoming event
    for k, v in headers.items():
        if any(h in k for h in ['content-type', 'cookie']):
            req_headers[k] = v

    # Construct the request
    url = f'https://{HOST}{path}'
    response = requests.request(
        method=method,
        url=url,
        headers=req_headers,
        data=body,
        params=query_params,
        stream=True
    )
    logger.info(f"Calling URL: {url}")
    # Process and return the response
    (headers, body, isBase64Encoded) = format_response(response)
    return {
        'statusCode': response.status_code,
        'body': body,
        'headers': headers,
        'isBase64Encoded': isBase64Encoded
    }