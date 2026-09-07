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

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1788720362445 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1788720362445")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1788720393007 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1788720393007")

# Script generated for node Machine Learning Curated
SqlQuery1682 = '''
SELECT
    st.sensorReadingTime,
    st.serialNumber,
    st.distanceFromObject,
    a.user,
    a.timestamp,
    a.x,
    a.y,
    a.z
FROM steptrainer st
INNER JOIN accelerometer a
    ON st.sensorReadingTime = a.timestamp
'''
MachineLearningCurated_node1788720426325 = sparkSqlQuery(glueContext, query = SqlQuery1682, mapping = {"steptrainer":StepTrainerTrusted_node1788720362445, "accelerometer":AccelerometerTrusted_node1788720393007}, transformation_ctx = "MachineLearningCurated_node1788720426325")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=MachineLearningCurated_node1788720426325, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1788720339562", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1788720494128 = glueContext.getSink(path="s3://stedi-lake-house-shamim/machine_learning/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1788720494128")
AmazonS3_node1788720494128.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
AmazonS3_node1788720494128.setFormat("json")
AmazonS3_node1788720494128.writeFrame(MachineLearningCurated_node1788720426325)
job.commit()