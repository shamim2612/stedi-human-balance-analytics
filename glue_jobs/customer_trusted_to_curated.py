import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Customer Trusted
CustomerTrusted_node1788669913048 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1788669913048")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1788669844244 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="accelerometer_trusted_node1788669844244")

# Script generated for node Customer Curated
SqlQuery1736 = '''
SELECT DISTINCT
    c.customername,
    c.email,
    c.phone,
    c.birthday,
    c.serialnumber,
    c.registrationdate,
    c.lastupdatedate,
    c.sharewithresearchasofdate,
    c.sharewithpublicasofdate,
    c.sharewithfriendsasofdate
FROM customer c
INNER JOIN accelerometer a
    ON c.email = a.user
'''
CustomerCurated_node1788718321980 = sparkSqlQuery(glueContext, query = SqlQuery1736, mapping = {"customer":CustomerTrusted_node1788669913048, "accelerometer":accelerometer_trusted_node1788669844244}, transformation_ctx = "CustomerCurated_node1788718321980")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=CustomerCurated_node1788718321980, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1788666081418", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1788670029012 = glueContext.getSink(path="s3://stedi-lake-house-shamim/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1788670029012")
AmazonS3_node1788670029012.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
AmazonS3_node1788670029012.setFormat("json")
AmazonS3_node1788670029012.writeFrame(CustomerCurated_node1788718321980)
job.commit()