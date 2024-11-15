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

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1731623356673 = glueContext.create_dynamic_frame.from_catalog(database="ryan_414_v2", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1731623356673")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1731623463960 = glueContext.create_dynamic_frame.from_catalog(database="ryan_414_v2", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1731623463960")

# Script generated for node SQL Query
SqlQuery11590 = '''
select stt.serialnumber, stt.distancefromobject, acc.x, acc.y, acc.z, acc.timestamp from stt 
join acc on stt.sensorreadingtime = acc.timestamp

'''
SQLQuery_node1731624220033 = sparkSqlQuery(glueContext, query = SqlQuery11590, mapping = {"acc":AccelerometerTrusted_node1731623356673, "stt":StepTrainerTrusted_node1731623463960}, transformation_ctx = "SQLQuery_node1731624220033")

# Script generated for node ML Curated
MLCurated_node1731623552912 = glueContext.getSink(path="s3://ryan-bucket-414/machine_learning/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MLCurated_node1731623552912")
MLCurated_node1731623552912.setCatalogInfo(catalogDatabase="ryan_414_v2",catalogTableName="machine_learning_curated")
MLCurated_node1731623552912.setFormat("json")
MLCurated_node1731623552912.writeFrame(SQLQuery_node1731624220033)
job.commit()