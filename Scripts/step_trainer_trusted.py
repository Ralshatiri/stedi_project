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

# Script generated for node customers curated
customerscurated_node1773688043597 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customers_curated", transformation_ctx="customerscurated_node1773688043597")

# Script generated for node step trainer landing
steptrainerlanding_node1773687907368 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="steptrainerlanding_node1773687907368")

# Script generated for node SQL Query
SqlQuery1794 = '''
SELECT DISTINCT
    s.sensorreadingtime,
    s.serialnumber,
    s.distancefromobject
FROM step_trainer_landing s
INNER JOIN customers_curated c
    ON s.serialnumber = c.serialnumber;
'''
SQLQuery_node1773688187835 = sparkSqlQuery(glueContext, query = SqlQuery1794, mapping = {"step_trainer_landing":steptrainerlanding_node1773687907368, "customers_curated":customerscurated_node1773688043597}, transformation_ctx = "SQLQuery_node1773688187835")

# Script generated for node step_trainer_trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1773688187835, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1773688037620", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
step_trainer_trusted_node1773688100833 = glueContext.getSink(path="s3://stedi-lake-house-for-learning/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="step_trainer_trusted_node1773688100833")
step_trainer_trusted_node1773688100833.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
step_trainer_trusted_node1773688100833.setFormat("json")
step_trainer_trusted_node1773688100833.writeFrame(SQLQuery_node1773688187835)
job.commit()