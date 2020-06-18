# Google Bucket Manifest
Repos for storing code to generate the Google dataflow template and consume pubsub subscription

## To build a template

```
python bucket_manifest_pipeline.py --runner DataflowRunner  --project "$project" --bucket "$bucket" --temp_location gs://"$temp_bucket"/temp  --template_location gs://"$temp_bucket"/templates/pipeline_template --region us-central1 --setup_file ./setup.py --service_account_email "${service_account}"
```
The output will be stored in `gs://"$temp_bucket"/templates/pipeline_template`

## To consume a subscription

```
python bucket_manifest/sub.py create_manifest --project_id $PROJECT --subscription_id ${PUBSUB_SUB} --n_expected_messages ${N_MESSAGES} --bucket_name ${OUT_BUCKET} --metadata_file ${METADATA_FILE}
```
where `METADATA_FILE` is tsv format and must contain `url` field so that it can be mapped to the one in subscription messages. The file can contain any other fields you want to put into the output manifest.