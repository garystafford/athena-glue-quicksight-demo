# Getting Started with Data Analysis on AWS using AWS Glue, Amazon Athena, and QuickSight

Code for the post, [Getting Started with Data Analysis on AWS using AWS Glue, Amazon Athena, and QuickSight](https://programmaticponderings.com/2020/01/05/getting-started-with-data-analysis-on-aws-using-s3-glue-athena-and-quicksight/).

## AWS CLI Commands

The following is a list of the AWS CLI commands, which are part of the post’s demonstration.

```bash
# step 0 (clone project)
git clone \
    --branch master --single-branch --depth 1 --no-tags \
    https://github.com/garystafford/athena-glue-quicksight-demo.git

# step 1 (change me)
BUCKET_SUFFIX="your-account-and-region"
DATA_BUCKET="smart-hub-data-${BUCKET_SUFFIX}"
SCRIPT_BUCKET="smart-hub-scripts-${BUCKET_SUFFIX}"
LOG_BUCKET="smart-hub-logs-${BUCKET_SUFFIX}"

# step 2 (cloudformation #1)
aws cloudformation create-stack \
    --stack-name smart-hub-athena-glue-stack \
    --template-body file://cloudformation/smart-hub-athena-glue.yml \
    --parameters ParameterKey=DataBucketName,ParameterValue=${DATA_BUCKET} \
                 ParameterKey=ScriptBucketName,ParameterValue=${SCRIPT_BUCKET} \
                 ParameterKey=LogBucketName,ParameterValue=${LOG_BUCKET} \
    --capabilities CAPABILITY_NAMED_IAM

# step 3 (copy raw data files to s3)
# location data
aws s3 cp data/locations/denver_co_1576656000.csv \
    s3://${DATA_BUCKET}/smart_hub_locations_csv/state=co/
aws s3 cp data/locations/palo_alto_ca_1576742400.csv \
    s3://${DATA_BUCKET}/smart_hub_locations_csv/state=ca/
aws s3 cp data/locations/portland_metro_or_1576742400.csv \
    s3://${DATA_BUCKET}/smart_hub_locations_csv/state=or/
aws s3 cp data/locations/stamford_ct_1576569600.csv \
    s3://${DATA_BUCKET}/smart_hub_locations_csv/state=ct/

# sensor mapping data
aws s3 cp data/mappings/ \
    s3://${DATA_BUCKET}/sensor_mappings_json/state=or/ \
    --recursive

# electrical usage data
aws s3 cp data/usage/2019-12-21/ \
    s3://${DATA_BUCKET}/smart_hub_data_json/dt=2019-12-21/ \
    --recursive
aws s3 cp data/usage/2019-12-22/ \
    s3://${DATA_BUCKET}/smart_hub_data_json/dt=2019-12-22/ \
    --recursive

# electricity rates data
aws s3 cp data/rates/ \
    s3://${DATA_BUCKET}/electricity_rates_xml/ \
    --recursive

# view raw s3 data files
aws s3 ls s3://${DATA_BUCKET}/ \
    --recursive --human-readable --summarize

# step 4 (package lambda functions)
pushd lambdas/athena-json-to-parquet-data || exit
zip -r package.zip index.py
popd || exit

pushd lambdas/athena-csv-to-parquet-locations || exit
zip -r package.zip index.py
popd || exit

pushd lambdas/athena-json-to-parquet-mappings || exit
zip -r package.zip index.py
popd || exit

pushd lambdas/athena-complex-etl-query || exit
zip -r package.zip index.py
popd || exit

pushd lambdas/athena-parquet-to-parquet-elt-data || exit
zip -r package.zip index.py
popd || exit

# step 5 (copy lambda packages to s3)
aws s3 cp lambdas/athena-json-to-parquet-data/package.zip \
    s3://${SCRIPT_BUCKET}/lambdas/athena_json_to_parquet_data/

aws s3 cp lambdas/athena-csv-to-parquet-locations/package.zip \
    s3://${SCRIPT_BUCKET}/lambdas/athena_csv_to_parquet_locations/

aws s3 cp lambdas/athena-json-to-parquet-mappings/package.zip \
    s3://${SCRIPT_BUCKET}/lambdas/athena_json_to_parquet_mappings/

aws s3 cp lambdas/athena-complex-etl-query/package.zip \
    s3://${SCRIPT_BUCKET}/lambdas/athena_complex_etl_query/

aws s3 cp lambdas/athena-parquet-to-parquet-elt-data/package.zip \
    s3://${SCRIPT_BUCKET}/lambdas/athena_parquet_to_parquet_elt_data/

# step 6 (cloudformation #2)
aws cloudformation create-stack \
    --stack-name smart-hub-lambda-stack \
    --template-body file://cloudformation/smart-hub-lambda.yml \
    --capabilities CAPABILITY_NAMED_IAM

# step 7 (run crawlers)
aws glue start-crawler --name smart-hub-locations-csv
aws glue start-crawler --name smart-hub-sensor-mappings-json
aws glue start-crawler --name smart-hub-data-json
aws glue start-crawler --name smart-hub-rates-xml

# crawler status
aws glue get-crawler-metrics \
    | jq -r '.CrawlerMetricsList[] | "\(.CrawlerName): \(.StillEstimating), \(.TimeLeftSeconds)"' \
    | grep "^smart-hub-[A-Za-z-]*"

# step 8 (invoke lambda functions)
aws lambda invoke \
    --function-name athena-json-to-parquet-data \
    response.json

aws lambda invoke \
    --function-name athena-csv-to-parquet-locations \
    response.json

aws lambda invoke \
    --function-name athena-json-to-parquet-mappings \
    response.json

# step 9 (copy etl job script to s3)
aws s3 cp glue-scripts/rates_xml_to_parquet.py \
    s3://${SCRIPT_BUCKET}/glue_scripts/

# step 10 (start etl job)
aws glue start-job-run --job-name rates-xml-to-parquet

# get status of most recent job (the one that is running)
aws glue get-job-run \
    --job-name rates-xml-to-parquet \
    --run-id "$(aws glue get-job-runs \
        --job-name rates-xml-to-parquet \
        | jq -r '.JobRuns[0].Id')"

# step 11 (run crawler)
aws glue start-crawler --name smart-hub-rates-parquet

# step 12 (invoke lambda function)
aws lambda invoke \
  --function-name athena-complex-etl-query \
  --payload "{ \"loc_id\": \"b6a8d42425fde548\",
  \"date_from\": \"2019-12-21\", \"date_to\": \"2019-12-22\"}" \
  response.json

# step 13 (run crawler)
aws glue start-crawler --name smart-hub-etl-tmp-output-parquet

# step 14 (invoke lambda function)
aws lambda invoke \
    --function-name athena-parquet-to-parquet-elt-data \
    response.json

# step 15 (run crawler)
aws glue start-crawler --name smart-hub-etl-output-parquet

# step 16 (fix 4 table's classification: Unknown)
database=smart_hub_data_catalog
tables=(smart_hub_locations_parquet sensor_mappings_parquet smart_hub_data_parquet etl_output_parquet)

for table in ${tables}; do
  fixed_table=$(aws glue get-table \
      --database-name "${database}" \
      --name "${table}" \
      | jq '.Table.Parameters["classification"] = "parquet" | del(.Table.DatabaseName) | del(.Table.CreateTime) | del(.Table.UpdateTime) | del(.Table.CreatedBy) | del(.Table.IsRegisteredWithLakeFormation)')

  fixed_table=$(echo "${fixed_table}" | jq .Table)

  aws glue update-table \
    --database-name "${database}" \
    --table-input "${fixed_table}"

  echo "table '${table}' fixed"
done

# get list of tables
aws glue get-tables \
    --database-name smart_hub_data_catalog \
    | jq -r '.TableList[].Name'

# delete demonstration resources

# delete s3 contents first
aws s3 rm s3://${DATA_BUCKET} --recursive
aws s3 rm s3://${SCRIPT_BUCKET} --recursive
aws s3 rm s3://${LOG_BUCKET} --recursive

# then, delete lambda cfn stack
aws cloudformation delete-stack --stack-name smart-hub-lambda-stack

# finally, delete athena-glue-s3 stack
aws cloudformation delete-stack --stack-name smart-hub-athena-glue-stack
```
