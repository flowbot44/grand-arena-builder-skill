import os
import json
import requests
import time

def get_api_key():
    """Gets the API key from an environment variable."""
    api_key = os.environ.get('MOKI_API_KEY')
    if not api_key:
        print("Error: MOKI_API_KEY environment variable not set.")
        print("Please set the MOKI_API_KEY environment variable with your API key.")
        return None
    return api_key

def get_graphql_query():
    """Reads the GraphQL query from query.txt."""
    try:
        with open('query.txt', 'r') as f:
            # The file content is a JSON string, so we need to parse it
            query_data = json.load(f)
            return query_data['query']
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Error reading or parsing query.txt: {e}")
        return None

def update_champions_data():
    """Fetches the latest trait data for champions and updates champions.json."""
    api_key = get_api_key()
    if not api_key:
        return

    graphql_query = get_graphql_query()
    if not graphql_query:
        return

    try:
        with open('champions.json', 'r') as f:
            champions = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Error reading or parsing champions.json: {e}")
        return

    updated_champions = []
    headers = {'X-API-Key': api_key}
    url = "https://marketplace-graphql.skymavis.com/graphql"

    for i, champion in enumerate(champions):
        champion_id = champion['id']
        print(f"Fetching data for champion {champion_id} ({i+1}/{len(champions)})...")

        variables = {
            "tokenId": str(champion_id),
            "tokenAddress": "0x47b5a7c2e4f07772696bbf8c8c32fe2b9eabd550",
            "includeLastSalePrice": False,
            "includeReceivedTimestamp": False
        }

        payload = {
            "operationName": "GetERC721TokenDetail",
            "variables": variables,
            "query": graphql_query
        }

        try:
            response = requests.post(url, headers=headers, json=payload)
            response.raise_for_status()
            data = response.json()

            new_traits = []
            if 'data' in data and data['data'] and data['data'].get('erc721Token') and 'attributes' in data['data']['erc721Token']:
                attributes = data['data']['erc721Token']['attributes']
                
                if isinstance(attributes, list): # The expected format
                    for attr in attributes:
                        if isinstance(attr, dict) and 'value' in attr:
                            new_traits.append(attr['value'])
                elif isinstance(attributes, dict): # The other format found in the error log
                    for key, value in attributes.items():
                        if isinstance(value, list):
                            new_traits.extend(value)
                        else:
                            new_traits.append(value)
                elif attributes is not None:
                     print(f"Warning: `attributes` is not a list or dict for champion {champion_id}. Attributes: {attributes}")

            updated_champion = {
                "id": champion_id,
                "name": champion['name'],
                "traits": new_traits if new_traits else champion.get('traits', [])
            }
            updated_champions.append(updated_champion)

        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for champion {champion_id}: {e}")
            updated_champions.append(champion)
        
        time.sleep(0.5)

    try:
        with open('champions_updated.json', 'w') as f:
            json.dump(updated_champions, f, indent=2)
        print("\nSuccessfully created champions_updated.json with the latest trait data.")
    except IOError as e:
        print(f"Error writing to champions_updated.json: {e}")


if __name__ == '__main__':
    update_champions_data()
