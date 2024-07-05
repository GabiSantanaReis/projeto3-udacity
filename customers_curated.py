import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

# Obter argumentos do trabalho
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Carregar dados do S3 (zona confiável)
customer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="customer_trusted"
)

accelerometer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_trusted"
)

# Filtrar clientes que concordaram em compartilhar seus dados para pesquisa
filtered_customers = Filter.apply(
    frame=customer_trusted,
    f=lambda row: row["shareWithResearchAsOfDate"] is not None,
    transformation_ctx="filtered_customers"
)

# Converter DynamicFrames para DataFrames para usar funções Spark
filtered_customers_df = filtered_customers.toDF()
accelerometer_trusted_df = accelerometer_trusted.toDF()

# Realizar Join para incluir apenas clientes que têm dados de acelerômetro
joined_df = filtered_customers_df.join(
    accelerometer_trusted_df,
    filtered_customers_df['email'] == accelerometer_trusted_df['user'],
    'inner'
).dropDuplicates()

# Converter de volta para DynamicFrame
joined_dynamic_frame = DynamicFrame.fromDF(joined_df, glueContext, "joined_dynamic_frame")

# Salvar dados no S3 (zona com curadoria)
sink = glueContext.getSink(
    path="s3://data-lake-estudo-udacity/customer/curated/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="sink"
)
sink.setCatalogInfo(
    catalogDatabase="stedi",
    catalogTableName="customers_curated"
)
sink.setFormat("glueparquet")
sink.writeFrame(joined_dynamic_frame)

# Finalizar o job
job.commit()
