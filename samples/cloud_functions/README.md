# Data Validation on Cloud Functions

DVT on Cloud Functions utilizes an HTTP trigger that accepts JSON data representing the DVT run configuration. Details on the JSON configuration is outlined below. The function returns a JSON string with the validation results. The function can also be configured to output results to BigQuery as a result handler.

Keep in mind that Cloud Functions (gen2) has a maximum timeout of 60 minutes which may be unsuitable for some validations. In this case, we recommend using Cloud Run as an alternative (24h timeout). Defaults for `--timeout` and `--memory` have been included as a guide below to apply your own settings.

### Quick Steps

To quickly deploy DVT, follow this script:

```
export PROJECT_ID=<PROJECT-ID>
export BUCKET=<BUCKET>
export REGION=<REGION>
export SERVICE_ACCOUNT=<SERVICE-ACCOUNT>

cd samples/functions/

zip -r data_validation.zip .
gsutil cp data_validation.zip gs://${BUCKET}/

gcloud functions deploy data-validation --region=${REGION} \
  --entry-point=main \
  --gen2 --runtime=python38 --trigger-http \
  --memory=512M \
  --timeout=600s \
  --source=gs://${BUCKET}/data_validation.zip \
  --service-account=${SERVICE_ACCOUNT} \
  --project=${PROJECT_ID}

rm data_validation.zip
rm requirements.txt
cd ../../
```

### JSON Configuration

Below is an example of the JSON configuration that can be passed to the Cloud Function. You can get the JSON content specific for your scenario by using our CLI and providing the argument to generate the JSON config file [`--config-file-json` or `-cj <filepath>.json`]. IMPORTANT: do not forget to make the necessary adjustments between JSON and Python objects, check [this link as a reference](https://python-course.eu/applications-python/json-and-python.php).

```json
{
   "config":{
      "type":"Column",
      "source_conn":{
         "source_type":"BigQuery",
         "project_id":"PROJECT_ID",
         "google_service_account_key_path":null
      },
      "target_conn":{
         "source_type":"BigQuery",
         "project_id":"PROJECT_ID",
         "google_service_account_key_path":null
      },
      "schema_name":"bigquery-public-data.new_york_citibike",
      "table_name":"citibike_trips",
      "target_schema_name":"bigquery-public-data.new_york_citibike",
      "target_table_name":"citibike_trips",
      "result_handler":{
         "type":"BigQuery",
         "project_id":"PROJECT_ID",
         "table_id":"TABLE_ID"
      },
      "filters":[],
      "aggregates":[
         {
            "source_column":null,
            "target_column":null,
            "field_alias":"count",
            "type":"count"
         }
      ]
   }
}
```