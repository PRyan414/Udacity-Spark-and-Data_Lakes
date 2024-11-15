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

# Script generated for node Accelerator Landing
AcceleratorLanding_node1729616895322 = glueContext.create_dynamic_frame.from_catalog(database="ryan_414_v2", table_name="accelerometer_landing", transformation_ctx="AcceleratorLanding_node1729616895322")

# Script generated for node Customer Trusted
CustomerTrusted_node1729616904686 = glueContext.create_dynamic_frame.from_catalog(database="ryan_414_v2", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1729616904686")

# Script generated for node Join
Join_node1729616934160 = Join.apply(frame1=CustomerTrusted_node1729616904686, frame2=AcceleratorLanding_node1729616895322, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1729616934160")

# Script generated for node Drop Fields and Duplicates
SqlQuery3958 = '''
select distinct customerName, email, phone, birthday, serialNumber, registrationdate, lastupdatedate, sharewithresearchasofdate, sharewithpublicasofdate, sharewithfriendsasofdate from myDataSource
'''
DropFieldsandDuplicates_node1729649646892 = sparkSqlQuery(glueContext, query = SqlQuery3958, mapping = {"myDataSource":Join_node1729616934160}, transformation_ctx = "DropFieldsandDuplicates_node1729649646892")

# Script generated for node Customer Curated
CustomerCurated_node1729616940583 = glueContext.getSink(path="s3://ryan-bucket-414/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerCurated_node1729616940583")
CustomerCurated_node1729616940583.setCatalogInfo(catalogDatabase="ryan_414_v2",catalogTableName="customer_curated")
CustomerCurated_node1729616940583.setFormat("json")
CustomerCurated_node1729616940583.writeFrame(DropFieldsandDuplicates_node1729649646892)
job.commit()