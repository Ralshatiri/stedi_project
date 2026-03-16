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

# Script generated for node accelerometer trusted
accelerometertrusted_node1773696152856 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="accelerometertrusted_node1773696152856")

# Script generated for node step trainer trusted
steptrainertrusted_node1773696019413 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="steptrainertrusted_node1773696019413")

# Script generated for node SQL Query
SqlQuery1774 = '''
SELECT
    s.sensorreadingtime,
    s.serialnumber,
    s.distancefromobject,
    a.x,
    a.y,
    a.z
FROM step_trainer_trusted s
INNER JOIN accelerometer_trusted a
    ON s.sensorreadingtime = a.timestamp;
'''
SQLQuery_node1773696198024 = sparkSqlQuery(glueContext, query = SqlQuery1774, mapping = {"step_trainer_trusted":steptrainertrusted_node1773696019413, "accelerometer_trusted":accelerometertrusted_node1773696152856}, transformation_ctx = "SQLQuery_node1773696198024")

# Script generated for node machine learning curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1773696198024, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1773696096076", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
machinelearningcurated_node1773696271957 = glueContext.getSink(path="s3://stedi-lake-house-for-learning/machine_learnin/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="machinelearningcurated_node1773696271957")
machinelearningcurated_node1773696271957.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
machinelearningcurated_node1773696271957.setFormat("json")
machinelearningcurated_node1773696271957.writeFrame(SQLQuery_node1773696198024)
job.commit()