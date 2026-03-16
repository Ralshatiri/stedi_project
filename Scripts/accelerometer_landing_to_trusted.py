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

# Script generated for node accelerometer landing
accelerometerlanding_node1773636126928 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="accelerometerlanding_node1773636126928")

# Script generated for node customer trusted
customertrusted_node1773636278402 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="customertrusted_node1773636278402")

# Script generated for node Join
SqlQuery1435 = '''
SELECT
    a.timestamp,
    a.user,
    a.x,
    a.y,
    a.z
FROM accelerometer_landing a
INNER JOIN customer_trusted c
    ON a.user = c.email
    AND a.timestamp >= c.sharewithresearchasofdate
'''
Join_node1773636797350 = sparkSqlQuery(glueContext, query = SqlQuery1435, mapping = {"accelerometer_landing":accelerometerlanding_node1773636126928, "customer_trusted":customertrusted_node1773636278402}, transformation_ctx = "Join_node1773636797350")

# Script generated for node accelerometer trusted
EvaluateDataQuality().process_rows(frame=Join_node1773636797350, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1773636626310", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
accelerometertrusted_node1773636887747 = glueContext.getSink(path="s3://stedi-lake-house-for-learning/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="accelerometertrusted_node1773636887747")
accelerometertrusted_node1773636887747.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
accelerometertrusted_node1773636887747.setFormat("json")
accelerometertrusted_node1773636887747.writeFrame(Join_node1773636797350)
job.commit()