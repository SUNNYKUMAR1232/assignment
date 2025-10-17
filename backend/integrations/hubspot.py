# hubspot.py
import datetime
import json
import secrets
import os
from fastapi import Request, HTTPException
from fastapi.responses import HTMLResponse
import httpx
import asyncio
import base64
import hashlib
from dotenv import load_dotenv

import requests
from integrations.integration_item import IntegrationItem

from redis_client import add_key_value_redis, get_value_redis, delete_key_redis

# Load environment variables from .env file
load_dotenv()

# HubSpot OAuth2 constants from environment variables
CLIENT_ID = os.getenv('HUBSPOT_CLIENT_ID', 'YOUR_HUBSPOT_CLIENT_ID')
CLIENT_SECRET = os.getenv('HUBSPOT_CLIENT_SECRET', 'YOUR_HUBSPOT_CLIENT_SECRET')
REDIRECT_URI = os.getenv('HUBSPOT_REDIRECT_URI', 'http://localhost:8000/integrations/hubspot/oauth2callback')
scopes = os.getenv('HUBSPOT_SCOPES', 'crm.objects.contacts.read crm.objects.contacts.write')
authorization_url = f'https://app.hubspot.com/oauth/authorize?client_id={CLIENT_ID}&scope={scopes}&redirect_uri={REDIRECT_URI}&response_type=code'
encoded_client_id_secret = base64.b64encode(f'{CLIENT_ID}:{CLIENT_SECRET}'.encode()).decode()


async def authorize_hubspot(user_id, org_id):
    state_data = {
        'state': secrets.token_urlsafe(32),
        'user_id': user_id,
        'org_id': org_id
    }
    encoded_state = base64.urlsafe_b64encode(json.dumps(state_data).encode('utf-8')).decode('utf-8')

    code_verifier = secrets.token_urlsafe(32)
    m = hashlib.sha256()
    m.update(code_verifier.encode('utf-8'))
    code_challenge = base64.urlsafe_b64encode(m.digest()).decode('utf-8').replace('=', '')

    auth_url = f'{authorization_url}&state={encoded_state}&code_challenge={code_challenge}&code_challenge_method=S256'
    await asyncio.gather(
        add_key_value_redis(f'hubspot_state:{org_id}:{user_id}', json.dumps(state_data), expire=600),
        add_key_value_redis(f'hubspot_verifier:{org_id}:{user_id}', code_verifier, expire=600),
    )
    return auth_url

async def oauth2callback_hubspot(request: Request):
    if request.query_params.get('error'):
        raise HTTPException(status_code=400, detail=request.query_params.get('error_description'))
    code = request.query_params.get('code')
    encoded_state = request.query_params.get('state')
    state_data = json.loads(base64.urlsafe_b64decode(encoded_state).decode('utf-8'))

    original_state = state_data.get('state')
    user_id = state_data.get('user_id')
    org_id = state_data.get('org_id')

    saved_state, code_verifier = await asyncio.gather(
        get_value_redis(f'hubspot_state:{org_id}:{user_id}'),
        get_value_redis(f'hubspot_verifier:{org_id}:{user_id}'),
    )

    if not saved_state or original_state != json.loads(saved_state).get('state'):
        raise HTTPException(status_code=400, detail='State does not match.')

    async with httpx.AsyncClient() as client:
        response, _, _ = await asyncio.gather(
            client.post(
                'https://api.hubapi.com/oauth/v1/token',
                data={
                    'grant_type': 'authorization_code',
                    'code': code,
                    'redirect_uri': REDIRECT_URI,
                    'client_id': CLIENT_ID,
                    'client_secret': CLIENT_SECRET,
                    'code_verifier': code_verifier.decode('utf-8'),
                },
                headers={
                    'Content-Type': 'application/x-www-form-urlencoded',
                }
            ),
            delete_key_redis(f'hubspot_state:{org_id}:{user_id}'),
            delete_key_redis(f'hubspot_verifier:{org_id}:{user_id}'),
        )

    await add_key_value_redis(f'hubspot_credentials:{org_id}:{user_id}', json.dumps(response.json()), expire=600)
    close_window_script = """
    <html>
        <script>
            window.close();
        </script>
    </html>
    """
    return HTMLResponse(content=close_window_script)

async def get_hubspot_credentials(user_id, org_id):
    credentials = await get_value_redis(f'hubspot_credentials:{org_id}:{user_id}')
    if not credentials:
        raise HTTPException(status_code=400, detail='No credentials found.')
    credentials = json.loads(credentials)
    await delete_key_redis(f'hubspot_credentials:{org_id}:{user_id}')
    return credentials

def create_integration_item_metadata_object(
    response_json: dict, item_type: str = 'Contact'
) -> IntegrationItem:
    """Creates an integration metadata object from HubSpot contact response"""
    properties = response_json.get('properties', {})
    
    # Extract contact name (firstname + lastname, or email as fallback)
    firstname = properties.get('firstname', '')
    lastname = properties.get('lastname', '')
    email = properties.get('email', '')
    
    if firstname or lastname:
        name = f"{firstname} {lastname}".strip()
    elif email:
        name = email
    else:
        name = f"Contact {response_json.get('id', 'Unknown')}"
    
    # Extract timestamps
    created_at = response_json.get('createdAt', None)
    updated_at = response_json.get('updatedAt', None)
    
    integration_item_metadata = IntegrationItem(
        id=response_json.get('id', None),
        name=name,
        type=item_type,
        creation_time=created_at,
        last_modified_time=updated_at,
        url=f"https://app.hubspot.com/contacts/{response_json.get('id')}" if response_json.get('id') else None,
    )
    
    return integration_item_metadata


def fetch_contacts(
    access_token: str, url: str, aggregated_response: list, after=None
) -> None:
    """Recursively fetch all HubSpot contacts with pagination"""
    params = {'limit': 100}
    if after:
        params['after'] = after
    
    headers = {'Authorization': f'Bearer {access_token}'}
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code == 200:
        response_data = response.json()
        results = response_data.get('results', [])
        
        for item in results:
            aggregated_response.append(item)
        
        # Check for pagination
        paging = response_data.get('paging', {})
        next_page = paging.get('next', {})
        after = next_page.get('after', None)
        
        if after:
            fetch_contacts(access_token, url, aggregated_response, after)
    else:
        print(f"Error fetching contacts: {response.status_code} - {response.text}")


async def get_items_hubspot(credentials) -> list[IntegrationItem]:
    """Fetches all contacts from HubSpot and returns as IntegrationItem objects"""
    credentials = json.loads(credentials)
    access_token = credentials.get('access_token')
    
    if not access_token:
        raise HTTPException(status_code=400, detail='No access token found in credentials.')
    
    # HubSpot Contacts API endpoint
    url = 'https://api.hubapi.com/crm/v3/objects/contacts'
    list_of_integration_item_metadata = []
    list_of_responses = []
    
    # Fetch all contacts with pagination
    fetch_contacts(access_token, url, list_of_responses)
    
    # Convert each contact to IntegrationItem
    for response in list_of_responses:
        list_of_integration_item_metadata.append(
            create_integration_item_metadata_object(response, 'Contact')
        )
    
    print(f'HubSpot contacts fetched: {len(list_of_integration_item_metadata)}')
    return list_of_integration_item_metadata