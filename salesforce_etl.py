import os
import time
import json
from dotenv import load_dotenv
import requests

load_dotenv()

SF_DOMAIN = os.getenv("SALESFORCE_DOMAIN")
SF_BEARER_TOKEN = os.getenv("BEARER_TOKEN_SALESFORCE")
API_UPSERT_URL = os.getenv("API_UPSERT_URL")
UPSERT_BEARER_TOKEN = os.getenv("BEARER_TOKEN_UPSERT")
PERIODICITY_MINUTES = int(os.getenv("PERIODICITY_MINUTES"))
PERIODICITY_SECONDS = PERIODICITY_MINUTES * 60

QUERY = "SELECT ArchivedDate,ArticleCreatedDate,ArticleMasterLanguage,ArticleNumber,ArticleTotalViewCount,CreatedDate,FirstPublishedDate,Id,IsDeleted,IsLatestVersion,VersionNumber,ValidationStatus,IsVisibleInApp,IsVisibleInCsp,IsVisibleInPkb,IsVisibleInPrm,KnowledgeArticleId,Language,LastModifiedById,LastModifiedDate,LastPublishedDate,PublishStatus,RecordTypeId,Answer__c,Question__c,retrievalAPISynced__c,wantSyncRetrievalAPI__c FROM Knowledge__kav WHERE PublishStatus = 'Online'"


def fetch_salesforce_data():
    url = f"https://{SF_DOMAIN}.my.salesforce.com/services/data/v57.0/query/?q={QUERY.replace(' ', '+')}"
    headers = {
        "Authorization": f"Bearer {SF_BEARER_TOKEN}"
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()["records"]


def transform_data(records):
    documents = []
    for record in records:
        doc = {
            "id": record["KnowledgeArticleId"],
            "text": f"Pregunta: {record['Question__c']}. Respuesta: {record['Answer__c']}",
            "metadata": {
                "ArticleNumber": record["ArticleNumber"],
                "Language": record["Language"],
                "RecordTypeId": record["RecordTypeId"],
                "Title": record["Title"],
                "Id": record["Id"],
                "ArticleCreatedDate": record["ArticleCreatedDate"],
                "CreatedDate": record["CreatedDate"],
                "FirstPublishedDate": record["FirstPublishedDate"],
                "LastModifiedDate": record["LastModifiedDate"],
                "LastPublishedDate": record["LastPublishedDate"],
                "ValidationStatus": record["ValidationStatus"],
                "VersionNumber": record["VersionNumber"]
            }
        }
        documents.append(doc)
    return {"documents": documents}


def push_data_to_external_api(json_data):
    headers = {
        "Authorization": f"Bearer {UPSERT_BEARER_TOKEN}",
        "Content-Type": "application/json"
    }
    response = requests.post(API_UPSERT_URL + "/upsert", headers=headers, json=json_data)
    response.raise_for_status()
    return response
def main():
    while True:
        try:
            records = fetch_salesforce_data()
            transformed_data = transform_data(records)
            push_data_to_external_api(transformed_data)
            print("ETL completado con éxito.")
        except Exception as e:
            print(f"Error en la ETL: {e}")
        time.sleep(PERIODICITY_SECONDS)


def run_etl_on_demand():
    try:
        records = fetch_salesforce_data()
        transformed_data = transform_data(records)
        push_data_to_external_api(transformed_data)
        print("ETL bajo demanda completado con éxito.")
    except Exception as e:
        print(f"Error en la ETL bajo demanda: {e}")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "on_demand":
        run_etl_on_demand()
    else:
        main()
