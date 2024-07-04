import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import re

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1720067733708 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://data-lake-estudo-udacity/customer/landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1720067733708")

# Script generated for node Amazon S3
AmazonS3_node1720067668282 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://data-lake-estudo-udacity/accelerometer/landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1720067668282")

# Script generated for node Change Schema
ChangeSchema_node1720067750478 = ApplyMapping.apply(frame=AmazonS3_node1720067733708, mappings=[("customername", "string", "customername", "string"), ("email", "string", "email", "string"), ("phone", "string", "phone", "string"), ("birthday", "string", "birthday", "string"), ("serialnumber", "string", "serialnumber", "string"), ("registrationdate", "long", "registrationdate", "long"), ("lastupdatedate", "long", "lastupdatedate", "long"), ("sharewithresearchasofdate", "long", "sharewithresearchasofdate", "long"), ("sharewithpublicasofdate", "long", "sharewithpublicasofdate", "long"), ("sharewithfriendsasofdate", "long", "sharewithfriendsasofdate", "long")], transformation_ctx="ChangeSchema_node1720067750478")

# Script generated for node Change Schema
ChangeSchema_node1720067730168 = ApplyMapping.apply(frame=AmazonS3_node1720067668282, mappings=[("user", "string", "user", "string"), ("timestamp", "long", "timestamp", "long"), ("x", "double", "x", "double"), ("y", "double", "y", "double"), ("z", "double", "z", "double")], transformation_ctx="ChangeSchema_node1720067730168")

# Script generated for node Filter
Filter_node1720067758932 = Filter.apply(frame=ChangeSchema_node1720067750478, f=lambda row: (not(row["sharewithresearchasofdate"] == 0)), transformation_ctx="Filter_node1720067758932")

# Script generated for node Join
ChangeSchema_node1720067730168DF = ChangeSchema_node1720067730168.toDF()
Filter_node1720067758932DF = Filter_node1720067758932.toDF()
Join_node1720067777814 = DynamicFrame.fromDF(ChangeSchema_node1720067730168DF.join(Filter_node1720067758932DF, (ChangeSchema_node1720067730168DF['user'] == Filter_node1720067758932DF['email']), "right"), glueContext, "Join_node1720067777814")

# Script generated for node Change Schema
ChangeSchema_node1720067956644 = ApplyMapping.apply(frame=Join_node1720067777814, mappings=[("user", "string", "user", "string"), ("timestamp", "long", "timestamp", "long"), ("x", "double", "x", "double"), ("y", "double", "y", "double"), ("z", "double", "z", "double")], transformation_ctx="ChangeSchema_node1720067956644")

# Script generated for node Amazon S3
AmazonS3_node1720067999858 = glueContext.getSink(path="s3://data-lake-estudo-udacity/customer/trusted/", connection_type="s3", updateBehavior="LOG", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1720067999858")
AmazonS3_node1720067999858.setCatalogInfo(catalogDatabase="stedi",catalogTableName="costumer_trusted")
AmazonS3_node1720067999858.setFormat("json")
AmazonS3_node1720067999858.writeFrame(ChangeSchema_node1720067956644)
job.commit()