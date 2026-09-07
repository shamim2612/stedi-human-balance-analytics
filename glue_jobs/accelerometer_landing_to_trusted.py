import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

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

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1788669844244 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1788669844244")
print("ACCELEROMETER LANDING COUNT =", AccelerometerLanding_node1788669844244.count())
print("CUSTOMER TRUSTED COUNT =", CustomerTrusted_node1788669913048.count())

# Script generated for node Join
Join_node1788669947827 = Join.apply(frame1=CustomerTrusted_node1788669913048, frame2=AccelerometerLanding_node1788669844244, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1788669947827")
print("JOIN COUNT =", Join_node1788669947827.count())

# Script generated for node Drop Fields
DropFields_node1788669985003 = ApplyMapping.apply(frame=Join_node1788669947827, mappings=[("user", "string", "user", "string"), ("timestamp", "long", "timestamp", "long"), ("x", "double", "x", "double"), ("y", "double", "y", "double"), ("z", "double", "z", "double")], transformation_ctx="DropFields_node1788669985003")
print("DROP FIELDS COUNT =", DropFields_node1788669985003.count())

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=DropFields_node1788669985003, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1788666081418", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1788670029012 = glueContext.getSink(path="s3://stedi-lake-house-shamim/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1788670029012")
AmazonS3_node1788670029012.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AmazonS3_node1788670029012.setFormat("json")
AmazonS3_node1788670029012.writeFrame(DropFields_node1788669985003)
job.commit()