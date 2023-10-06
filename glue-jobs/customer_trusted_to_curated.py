import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1695935239367 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://nnamdifirstbucket/stediproject/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerTrusted_node1695935239367",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://nnamdifirstbucket/stediproject/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1",
)

# Script generated for node Join
Join_node1695935347967 = Join.apply(
    frame1=CustomerTrusted_node1,
    frame2=AccelerometerTrusted_node1695935239367,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1695935347967",
)

# Script generated for node Drop Accelerometer Fields
DropAccelerometerFields_node1695935389705 = DropFields.apply(
    frame=Join_node1695935347967,
    paths=["x", "user", "y", "timeStamp", "z"],
    transformation_ctx="DropAccelerometerFields_node1695935389705",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1695948162207 = DynamicFrame.fromDF(
    DropAccelerometerFields_node1695935389705.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1695948162207",
)

# Script generated for node Customer Curated
CustomerCurated_node2 = glueContext.getSink(
    path="s3://nnamdifirstbucket/stediproject/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerCurated_node2",
)
CustomerCurated_node2.setCatalogInfo(
    catalogDatabase="stedi_project", catalogTableName="customer_curated"
)
CustomerCurated_node2.setFormat("json")
CustomerCurated_node2.writeFrame(DropDuplicates_node1695948162207)
job.commit()
