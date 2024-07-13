import logging
import os
import psycopg2
import requests
import azure.functions as func

def push_data_to_db(normalized_data):
    try:
        conn = psycopg2.connect(
            dbname="test",
            user="postgres",
            password="",
            host="localhost",
            port="5432"
            )
        cursor = conn.cursor()

        # Insert data into PostgreSQL
        for item in normalized_data:
            cursor.execute("""
                INSERT INTO test.objects (id, name, data_color, data_capacity)
                VALUES (%s, %s, %s, %s)
            """, (item["id"], item['name'], item['data_color'], item['data_capacity']))

        conn.commit()
    except psycopg2.Error as e:
        conn.rollback()
        logging.error(f"Database error: {e}")
        return func.HttpResponse(f"Database error: {e}", status_code=500)
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

    return func.HttpResponse("Inserted", status_code=200)
    
def getApiData():
    api_url = 'https://api.restful-api.dev/objects/1'
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        data = response.json()
        return data
    except requests.RequestException as e:
        logging.error(f"API request failed: {e}")
        return func.HttpResponse(f"API request failed: {e}", status_code=500)
    except ValueError as e:
        logging.error(f"Failed to parse JSON response: {e}")
        return func.HttpResponse(f"Failed to parse JSON response: {e}", status_code=500)

def normalizeHash(data):
    normalized_data = []
    for item in data:
        normalized_item = {
            "id": item["id"],
            "name": item["name"],
            "data_color": item["data"]["color"] if item["data"] else None,
            "data_capacity": item["data"]["capacity"] if item["data"] else None
        }
        normalized_data.append(normalized_item)
    return normalized_data

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    apiData = getApiData()
    normalized_data = normalizeHash(apiData)
    push_data_to_db(normalized_data)
    
