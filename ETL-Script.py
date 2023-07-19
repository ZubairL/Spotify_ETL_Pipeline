import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1689706077089 = glueContext.create_dynamic_frame.from_catalog(
    database="input",
    table_name="spotifyspotify_csv",
    transformation_ctx="AWSGlueDataCatalog_node1689706077089",
)

# Script generated for node apply_sp0tuify
apply_sp0tuify_node1689706092455 = ApplyMapping.apply(
    frame=AWSGlueDataCatalog_node1689706077089,
    mappings=[
        ("song_name", "string", "song_name", "string"),
        ("artist_name", "string", "artist_name", "string"),
        ("danceability", "double", "danceability", "double"),
        ("energy", "double", "energy", "double"),
        ("speechiness", "double", "speechiness", "double"),
        ("instrumentalness", "string", "instrumentalness", "string"),
        ("tempo", "double", "tempo", "double"),
    ],
    transformation_ctx="apply_sp0tuify_node1689706092455",
)

# Script generated for node Amazon S3
AmazonS3_node1689706125535 = glueContext.write_dynamic_frame.from_options(
    frame=apply_sp0tuify_node1689706092455,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://zubairspotifybucket/output/spotify_data_parquet/",
        "partitionKeys": [],
    },
    format_options={"compression": "uncompressed"},
    transformation_ctx="AmazonS3_node1689706125535",
)

job.commit()
