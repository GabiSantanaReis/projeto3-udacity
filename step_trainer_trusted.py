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

# Script generated for node Amazon S3
AmazonS3_node1720069397928 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://data-lake-estudo-udacity/step_trainer/landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1720069397928")

# Script generated for node Change Schema
ChangeSchema_node1720067750478 = ApplyMapping.apply(frame=AmazonS3_node1720067733708, mappings=[("customername", "string", "customername", "string"), ("email", "string", "email", "string"), ("phone", "string", "phone", "string"), ("birthday", "string", "birthday", "string"), ("serialnumber", "string", "serialnumber", "string"), ("registrationdate", "bigint", "registrationdate", "long"), ("lastupdatedate", "bigint", "lastupdatedate", "long"), ("sharewithresearchasofdate", "bigint", "sharewithresearchasofdate", "long"), ("sharewithpublicasofdate", "bigint", "sharewithpublicasofdate", "long"), ("sharewithfriendsasofdate", "bigint", "sharewithfriendsasofdate", "long")], transformation_ctx="ChangeSchema_node1720067750478")

# Script generated for node Change Schema
ChangeSchema_node1720067730168 = ApplyMapping.apply(frame=AmazonS3_node1720067668282, mappings=[("user", "string", "user", "string"), ("timestamp", "bigint", "timestamp", "long"), ("x", "double", "x", "double"), ("y", "double", "y", "double"), ("z", "double", "z", "double")], transformation_ctx="ChangeSchema_node1720067730168")

# Script generated for node Change Schema
ChangeSchema_node1720069433325 = ApplyMapping.apply(frame=AmazonS3_node1720069397928, mappings=[("sensorreadingtime", "bigint", "sensorreadingtime", "long"), ("serialnumber", "string", "serialnumber", "string"), ("distancefromobject", "int", "distancefromobject", "int")], transformation_ctx="ChangeSchema_node1720069433325")

# Script generated for node Filter
Filter_node1720067758932 = Filter.apply(frame=ChangeSchema_node1720067750478, f=lambda row: (not(row["sharewithresearchasofdate"] == 0)), transformation_ctx="Filter_node1720067758932")

# Script generated for node Join
ChangeSchema_node1720067730168DF = ChangeSchema_node1720067730168.toDF()
Filter_node1720067758932DF = Filter_node1720067758932.toDF()
Join_node1720067777814 = DynamicFrame.fromDF(ChangeSchema_node1720067730168DF.join(Filter_node1720067758932DF, (ChangeSchema_node1720067730168DF['user'] == Filter_node1720067758932DF['email']), "right"), glueContext, "Join_node1720067777814")

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1720069558857 = ApplyMapping.apply(frame=Join_node1720067777814, mappings=[("user", "string", "right_user", "string"), ("timestamp", "long", "right_timestamp", "long"), ("x", "double", "right_x", "double"), ("y", "double", "right_y", "double"), ("z", "double", "right_z", "double"), ("registrationdate", "long", "right_registrationdate", "long"), ("customername", "string", "right_customername", "string"), ("birthday", "string", "right_birthday", "string"), ("sharewithfriendsasofdate", "long", "right_sharewithfriendsasofdate", "long"), ("sharewithpublicasofdate", "long", "right_sharewithpublicasofdate", "long"), ("lastupdatedate", "long", "right_lastupdatedate", "long"), ("email", "string", "right_email", "string"), ("serialnumber", "string", "right_serialnumber", "string"), ("phone", "string", "right_phone", "string"), ("sharewithresearchasofdate", "long", "right_sharewithresearchasofdate", "long")], transformation_ctx="RenamedkeysforJoin_node1720069558857")

# Script generated for node Join
ChangeSchema_node1720069433325DF = ChangeSchema_node1720069433325.toDF()
RenamedkeysforJoin_node1720069558857DF = RenamedkeysforJoin_node1720069558857.toDF()
Join_node1720069435648 = DynamicFrame.fromDF(ChangeSchema_node1720069433325DF.join(RenamedkeysforJoin_node1720069558857DF, (ChangeSchema_node1720069433325DF['serialnumber'] == RenamedkeysforJoin_node1720069558857DF['right_serialnumber']) & (ChangeSchema_node1720069433325DF['sensorreadingtime'] == RenamedkeysforJoin_node1720069558857DF['right_timestamp']), "left"), glueContext, "Join_node1720069435648")

# Script generated for node Amazon S3
AmazonS3_node1720069693771 = glueContext.getSink(path="s3://data-lake-estudo-udacity/step_trainer/trusted/", connection_type="s3", updateBehavior="LOG", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1720069693771")
AmazonS3_node1720069693771.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
AmazonS3_node1720069693771.setFormat("json")
AmazonS3_node1720069693771.writeFrame(Join_node1720069435648)
job.commit()