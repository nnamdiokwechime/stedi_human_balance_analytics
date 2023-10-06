import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1695931233532 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://nnamdifirstbucket/stediproject/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerLanding_node1695931233532",
)

# Script generated for node Customer Landing
CustomerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://nnamdifirstbucket/stediproject/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="CustomerLanding_node1",
)

# Script generated for node Join
Join_node1695931288193 = Join.apply(
    frame1=CustomerLanding_node1,
    frame2=AccelerometerLanding_node1695931233532,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1695931288193",
)

# Script generated for node PrivacyFilter
PrivacyFilter_node1695930683885 = Filter.apply(
    frame=Join_node1695931288193,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="PrivacyFilter_node1695930683885",
)

# Script generated for node Drop Customer Columns
DropCustomerColumns_node1695931386604 = DropFields.apply(
    frame=PrivacyFilter_node1695930683885,
    paths=[
        "customerName",
        "email",
        "phone",
        "birthDay",
        "serialNumber",
        "registrationDate",
        "lastUpdateDate",
        "shareWithResearchAsOfDate",
        "shareWithPublicAsOfDate",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropCustomerColumns_node1695931386604",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node2 = glueContext.getSink(
    path="s3://nnamdifirstbucket/stediproject/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AccelerometerTrusted_node2",
)
AccelerometerTrusted_node2.setCatalogInfo(
    catalogDatabase="stedi_project", catalogTableName="accelerometer_trusted"
)
AccelerometerTrusted_node2.setFormat("json")
AccelerometerTrusted_node2.writeFrame(DropCustomerColumns_node1695931386604)
job.commit()
