import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    's3_output_path',
    'source_glue_database',
    'source_glue_table'
])

s3_output_path = args['s3_output_path']
source_glue_database = args['source_glue_database']
source_glue_table = args['source_glue_table']

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

datasource0 = glueContext. \
    create_dynamic_frame. \
    from_catalog(database=source_glue_database,
                 table_name=source_glue_table,
                 transformation_ctx="datasource0")

applymapping1 = ApplyMapping.apply(
    frame=datasource0,
    mappings=[("from", "string", "from", "string"),
              ("to", "string", "to", "string"),
              ("type", "string", "type", "string"),
              ("rate", "double", "rate", "double"),
              ("year", "int", "year", "int"),
              ("month", "int", "month", "int"),
              ("state", "string", "state", "string")],
    transformation_ctx="applymapping1")

resolvechoice2 = ResolveChoice.apply(
    frame=applymapping1,
    choice="make_struct",
    transformation_ctx="resolvechoice2")

dropnullfields3 = DropNullFields.apply(
    frame=resolvechoice2,
    transformation_ctx="dropnullfields3")

# coalesce parquet into one
# https://github.com/aws-samples/aws-glue-samples/blob/master/FAQ_and_How_to.md
partitioned_dataframe = dropnullfields3.toDF().repartition(1)
partitioned_dynamicframe = DynamicFrame.fromDF(partitioned_dataframe, glueContext, "partitioned_df")

datasink4 = glueContext.write_dynamic_frame.from_options(
    frame=partitioned_dynamicframe,
    connection_type="s3",
    connection_options={
        "path": s3_output_path,
        "groupFiles": "inPartition",
        "groupSize": 1024 * 1024,
        "partitionKeys": ["state"]
    },
    format="parquet",
    transformation_ctx="datasink4")

job.commit()
