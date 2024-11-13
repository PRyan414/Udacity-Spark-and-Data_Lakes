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

# Script generated for node Step Trainer Landing Table
StepTrainerLandingTable_node1731003574275 = glueContext.create_dynamic_frame.from_catalog(database="ryan_414_v2", table_name="step_trainer_landing", transformation_ctx="StepTrainerLandingTable_node1731003574275")

# Script generated for node Customer Curated Table
CustomerCuratedTable_node1731003572559 = glueContext.create_dynamic_frame.from_catalog(database="ryan_414_v2", table_name="customer_curated", transformation_ctx="CustomerCuratedTable_node1731003572559")

# Script generated for node SQL Query
SqlQuery8778 = '''
select * from s
join c on s.serialnumber = c.serialnumber
'''
SQLQuery_node1731273563810 = sparkSqlQuery(glueContext, query = SqlQuery8778, mapping = {"s":StepTrainerLandingTable_node1731003574275, "c":CustomerCuratedTable_node1731003572559}, transformation_ctx = "SQLQuery_node1731273563810")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1731004545742 = glueContext.getSink(path="s3://ryan-bucket-414/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="StepTrainerTrusted_node1731004545742")
StepTrainerTrusted_node1731004545742.setCatalogInfo(catalogDatabase="ryan_414_v2",catalogTableName="step_trainer_trusted")
StepTrainerTrusted_node1731004545742.setFormat("json")
StepTrainerTrusted_node1731004545742.writeFrame(SQLQuery_node1731273563810)
job.commit()