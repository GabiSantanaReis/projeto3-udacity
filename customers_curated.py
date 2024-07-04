import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1720064214536 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://data-lake-estudo-udacity/customer/landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1720064214536")

# Script generated for node Amazon S3
AmazonS3_node1720066292426 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://data-lake-estudo-udacity/accelerometer/landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1720066292426")

# Script generated for node Change Schema
ChangeSchema_node1720064255841 = ApplyMapping.apply(frame=AmazonS3_node1720064214536, mappings=[("customername", "string", "customername", "string"), ("email", "string", "email", "string"), ("phone", "string", "phone", "string"), ("birthday", "string", "birthday", "string"), ("serialnumber", "string", "serialnumber", "string"), ("registrationdate", "long", "registrationdate", "long"), ("lastupdatedate", "long", "lastupdatedate", "long"), ("sharewithpublicasofdate", "long", "sharewithpublicasofdate", "long"), ("sharewithresearchasofdate", "long", "sharewithresearchasofdate", "long"), ("sharewithfriendsasofdate", "long", "sharewithfriendsasofdate", "long")], transformation_ctx="ChangeSchema_node1720064255841")

# Script generated for node Change Schema
ChangeSchema_node1720066915773 = ApplyMapping.apply(frame=AmazonS3_node1720066292426, mappings=[("user", "string", "user", "string"), ("timestamp", "long", "timestamp", "long"), ("x", "double", "x", "double"), ("y", "double", "y", "double"), ("z", "double", "z", "double")], transformation_ctx="ChangeSchema_node1720066915773")

# Script generated for node Filter
Filter_node1720064303266 = Filter.apply(frame=ChangeSchema_node1720064255841, f=lambda row: (not(row["sharewithresearchasofdate"] == 0)), transformation_ctx="Filter_node1720064303266")

# Script generated for node Join
Join_node1720066922432 = Join.apply(frame1=ChangeSchema_node1720066915773, frame2=Filter_node1720064303266, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1720066922432")

# Script generated for node Amazon S3
AmazonS3_node1720067012286 = glueContext.getSink(path="s3://data-lake-estudo-udacity/customer/curated/", connection_type="s3", updateBehavior="LOG", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1720067012286")
AmazonS3_node1720067012286.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customers_curated")
AmazonS3_node1720067012286.setFormat("glueparquet", compression="snappy")
AmazonS3_node1720067012286.writeFrame(Join_node1720066922432)
job.commit()