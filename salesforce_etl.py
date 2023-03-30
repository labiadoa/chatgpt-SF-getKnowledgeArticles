import os
import json
import time
from datetime import datetime
import requests
from simple_salesforce import Salesforce

# Variables de entorno
SALESFORCE_USERNAME = os.environ["SALESFORCE_USERNAME"]
SALESFORCE_PASSWORD = os.environ["SALESFORCE_PASSWORD"]
SALESFORCE_SECURITY_TOKEN = os.environ["SALESFORCE_SECURITY_TOKEN"]
SALESFORCE_ORG_TYPE = os.environ["SALESFORCE_ORG_TYPE"]
API_UPSERT_URL = os.environ["API_UPSERT_URL"]
BEARER_TOKEN_UPSERT = os.environ["BEARER_TOKEN_UPSERT"]
PERIODICITY_MINUTES = int(os.environ["PERIODICITY_MINUTES"])
INSERTED_IDS_FILE = "inserted_ids.json"

# Determinar la URL de autenticación según SALESFORCE_ORG_TYPE
if SALESFORCE_ORG_TYPE.lower() == "test":
    SALESFORCE_LOGIN_URL = "https://test.salesforce.com"
elif SALESFORCE_ORG_TYPE.lower() == "production":
    SALESFORCE_LOGIN_URL = "https://login.salesforce.com"
else:
    raise ValueError("SALESFORCE_ORG_TYPE debe ser 'test' o 'production'")

# Conectar a Salesforce
sf = Salesforce(
    username=SALESFORCE_USERNAME,
    password=SALESFORCE_PASSWORD,
    security_token=SALESFORCE_SECURITY_TOKEN,
    sandbox=SALESFORCE_ORG_TYPE.lower() == "test"
)

def load_inserted_ids():
    if os.path.exists(INSERTED_IDS_FILE):
        with open(INSERTED_IDS_FILE, "r") as file:
            return json.load(file)
    return {}

def save_inserted_ids(inserted_ids):
    with open(INSERTED_IDS_FILE, "w") as file:
        json.dump(inserted_ids, file)

def fetch_salesforce_data():
    query = """SELECT ArchivedDate,ArticleCreatedDate,ArticleMasterLanguage,ArticleNumber,ArticleTotalViewCount,CreatedDate,FirstPublishedDate,Id,IsDeleted,IsLatestVersion,VersionNumber,ValidationStatus,IsVisibleInApp,IsVisibleInCsp,IsVisibleInPkb,IsVisibleInPrm,KnowledgeArticleId,Language,LastModifiedById,LastModifiedDate,LastPublishedDate,PublishStatus,RecordTypeId,Answer__c,Question__c,retrievalAPISynced__c,wantSyncRetrievalAPI__c FROM Knowledge__kav WHERE PublishStatus = 'Online'"""
    result = sf.query_all(query)
    return result["records"]

def create_documents(articles, inserted_ids):
    documents = []
    for article in articles:
        if article["Id"] not in inserted_ids:
            document = {
                "id": article["KnowledgeArticleId"],
                "text": f"Pregunta: {article['Question__c']}. Respuesta: {article['Answer__c']}",
                "metadata": {
                    "ArticleNumber": article["ArticleNumber"],
                    "Language": article["Language"],
                    "RecordTypeId": article["RecordTypeId"],
                    "Title": article["Title"],
                    "Id": article["Id"],
                    "ArticleCreatedDate": article["ArticleCreatedDate"],
                    "CreatedDate": article["CreatedDate"],
                    "FirstPublishedDate": article["FirstPublishedDate"],
                    "LastModifiedDate": article["LastModifiedDate"],
                    "LastPublishedDate": article["LastPublishedDate"],
                    "ValidationStatus": article["ValidationStatus"],
                    "VersionNumber": article["VersionNumber"]
                }
            }
            documents.append(document)
    return documents

def upsert_documents(documents):
    headers = {
        "Authorization": f"Bearer {BEARER_TOKEN_UPSERT}",
        "Content-Type": "application/json"
    }
    response = requests.post(
        API_UPSERT_URL,
        headers=headers,
        json={"documents": documents}
    )

    if response.status_code != 200:
        response.raise_for_status()

    return response

def log_results(articles, upsert_response):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    num_articles_extracted = len(articles)
    num_articles_inserted = len(upsert_response.json()["ids"])

    with open("etl_results.log", "a") as log_file:
        log_file.write(f"{timestamp} - Articles extracted: {num_articles_extracted}, Articles inserted: {num_articles_inserted}\n")

def run_etl():
    inserted_ids = load_inserted_ids()
    articles = fetch_salesforce_data()
    documents = create_documents(articles, inserted_ids)
    upsert_response = upsert_documents(documents)
    log_results(articles, upsert_response)

    new_inserted_ids = upsert_response.json()["ids"]
    for article_id, knowledge_article_id in zip([doc["metadata"]["Id"] for doc in documents], new_inserted_ids):
        inserted_ids[article_id] = knowledge_article_id
    save_inserted_ids(inserted_ids)

if __name__ == "__main__":
    while True:
        run_etl()
        time.sleep(PERIODICITY_MINUTES * 60)
