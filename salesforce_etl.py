import os
import requests
from flask import Flask, request, jsonify
from simple_salesforce import Salesforce
import json

app = Flask(__name__)

SALESFORCE_USERNAME = os.environ.get("SALESFORCE_USERNAME")
SALESFORCE_PASSWORD = os.environ.get("SALESFORCE_PASSWORD")
SALESFORCE_SECURITY_TOKEN = os.environ.get("SALESFORCE_SECURITY_TOKEN")
SALESFORCE_ORG_TYPE = os.environ.get("SALESFORCE_ORG_TYPE")

if SALESFORCE_ORG_TYPE == "Test":
    SALESFORCE_DOMAIN = "test"
else:
    SALESFORCE_DOMAIN = "login"

sf = Salesforce(
    username=SALESFORCE_USERNAME,
    password=SALESFORCE_PASSWORD,
    security_token=SALESFORCE_SECURITY_TOKEN,
    domain=SALESFORCE_DOMAIN,
)

@app.route('/salesforce-etl', methods=['POST'])
def salesforce_etl():
    url = os.environ.get("EXTERNAL_API_URL")
    bearer_token = os.environ.get("EXTERNAL_API_BEARER_TOKEN")

    query = "SELECT ArchivedDate,ArticleCreatedDate,ArticleMasterLanguage,ArticleNumber,ArticleTotalViewCount,CreatedDate,FirstPublishedDate,Id,IsDeleted,IsLatestVersion,VersionNumber,ValidationStatus,IsVisibleInApp,IsVisibleInCsp,IsVisibleInPkb,IsVisibleInPrm,KnowledgeArticleId,Language,LastModifiedById,LastModifiedDate,LastPublishedDate,PublishStatus,RecordTypeId,Answer__c,Question__c,retrievalAPISynced__c,wantSyncRetrievalAPI__c FROM Knowledge__kav WHERE PublishStatus = 'Online'"
    sf_records = sf.query_all(query)['records']

    documents = []
    for record in sf_records:
        if not record['retrievalAPISynced__c']:
            document = {
                "id": record['KnowledgeArticleId'],
                "text": f"Pregunta: {record['Question__c']} Respuesta: {record['Answer__c']}",
                "metadata": {
                    "ArticleNumber": record['ArticleNumber'],
                    "Language": record['Language'],
                    "RecordTypeId": record['RecordTypeId'],
                    "Title": record['Title'],
                    "Id": record['Id'],
                    "ArticleCreatedDate": record['ArticleCreatedDate'],
                    "CreatedDate": record['CreatedDate'],
                    "FirstPublishedDate": record['FirstPublishedDate'],
                    "LastModifiedDate": record['LastModifiedDate'],
                    "LastPublishedDate": record['LastPublishedDate'],
                    "ValidationStatus": record['ValidationStatus'],
                    "VersionNumber": record['VersionNumber']
                }
            }
            documents.append(document)

    if not documents:
        return jsonify({"message": "No new records to process."}), 200

    headers = {"Authorization": f"Bearer {bearer_token}"}
    data = {"documents": documents}
    response = requests.post(url + "/upsert", headers=headers, json=data)

    if response.status_code == 200:
        synced_ids = response.json().get("ids", [])
        for article_id in synced_ids:
            sf.Knowledge__kav.update(article_id, {'retrievalAPISynced__c': True})

        return jsonify({"message": f"Processed {len(documents)} records, upserted {len(synced_ids)} records in the external API."}), 200
    else:
        return jsonify({"message": f"Error upserting records in the external API. Status code: {response.status_code}, Response: {response.text}"}), 500


if __name__ == '__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
