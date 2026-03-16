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
accelerometertrusted_node1773686430935 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="accelerometertrusted_node1773686430935")

# Script generated for node customer trusted
customertrusted_node1773686371884 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="customertrusted_node1773686371884")

# Script generated for node SQL Query
SqlQuery1693 = '''
SELECT DISTINCT
    c.serialnumber,
    c.sharewithpublicasofdate,
    c.birthday,
    c.registrationdate,
    c.sharewithresearchasofdate,
    c.customername,
    c.email,
    c.lastupdatedate,
    c.phone,
    c.sharewithfriendsasofdate FROM customer_trusted c JOIN accelerometer_trusted a ON c.email=a.user;
'''
SQLQuery_node1773686412541 = sparkSqlQuery(glueContext, query = SqlQuery1693, mapping = {"customer_trusted":customertrusted_node1773686371884, "accelerometer_trusted":accelerometertrusted_node1773686430935}, transformation_ctx = "SQLQuery_node1773686412541")

# Script generated for node customers curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1773686412541, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1773686309705", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
customerscurated_node1773686738549 = glueContext.getSink(path="s3://stedi-lake-house-for-learning/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customerscurated_node1773686738549")
customerscurated_node1773686738549.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customers_curated")
customerscurated_node1773686738549.setFormat("json")
customerscurated_node1773686738549.writeFrame(SQLQuery_node1773686412541)
job.commit()