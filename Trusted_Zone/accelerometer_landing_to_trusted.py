import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
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

# Script generated for node Customer Trusted
CustomerTrusted_node1730408111835 = glueContext.create_dynamic_frame.from_catalog(database="ryan_414_v2", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1730408111835")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1730408074101 = glueContext.create_dynamic_frame.from_catalog(database="ryan_414_v2", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1730408074101")

# Script generated for node SQL Query
SqlQuery11145 = '''
select acl.x, acl.y, acl.z, acl.timestamp,
ctt.serialnumber, ctt.registrationdate, ctt.lastupdatedate,
ctt.sharewithresearchasofdate
from acl join ctt on acl.user = ctt.email
'''
SQLQuery_node1731626455876 = sparkSqlQuery(glueContext, query = SqlQuery11145, mapping = {"ctt":CustomerTrusted_node1730408111835, "acl":AccelerometerLanding_node1730408074101}, transformation_ctx = "SQLQuery_node1731626455876")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1730408234769 = glueContext.getSink(path="s3://ryan-bucket-414/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1730408234769")
AccelerometerTrusted_node1730408234769.setCatalogInfo(catalogDatabase="ryan_414_v2",catalogTableName="accelerometer_trusted")
AccelerometerTrusted_node1730408234769.setFormat("json")
AccelerometerTrusted_node1730408234769.writeFrame(SQLQuery_node1731626455876)
job.commit()
