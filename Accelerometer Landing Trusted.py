import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Accelerator Landing
AcceleratorLanding_node1729616895322 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://ryan-bucket-414/accelerometer/landing/"], "recurse": True}, transformation_ctx="AcceleratorLanding_node1729616895322")

# Script generated for node Customer Trusted
CustomerTrusted_node1729616904686 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://ryan-bucket-414/customer/trusted/"], "recurse": True}, transformation_ctx="CustomerTrusted_node1729616904686")

# Script generated for node Join
Join_node1729616934160 = Join.apply(frame1=CustomerTrusted_node1729616904686, frame2=AcceleratorLanding_node1729616895322, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1729616934160")

# Script generated for node Drop Fields
DropFields_node1729619273154 = DropFields.apply(frame=Join_node1729616934160, paths=["customername", "email", "phone", "birthday", "serialnumber", "registrationdate", "lastupdatedate", "sharewithresearchasofdate", "sharewithpublicasofdate", "sharewithfriendsasofdate", "timestamp"], transformation_ctx="DropFields_node1729619273154")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1729616940583 = glueContext.getSink(path="s3://ryan-bucket-414/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1729616940583")
AccelerometerTrusted_node1729616940583.setCatalogInfo(catalogDatabase="ryan_414_v2",catalogTableName="accelerometer_trusted")
AccelerometerTrusted_node1729616940583.setFormat("json")
AccelerometerTrusted_node1729616940583.writeFrame(DropFields_node1729619273154)
job.commit()