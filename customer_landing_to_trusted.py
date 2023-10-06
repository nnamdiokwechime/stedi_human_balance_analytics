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

# Script generated for node PrivacyFilter
PrivacyFilter_node1695930683885 = Filter.apply(
    frame=CustomerLanding_node1,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="PrivacyFilter_node1695930683885",
)

# Script generated for node Customer Trusted
CustomerTrusted_node2 = glueContext.getSink(
    path="s3://nnamdifirstbucket/stediproject/customer/trusted/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerTrusted_node2",
)
CustomerTrusted_node2.setCatalogInfo(
    catalogDatabase="stedi_project", catalogTableName="customer_trusted"
)
CustomerTrusted_node2.setFormat("json")
CustomerTrusted_node2.writeFrame(PrivacyFilter_node1695930683885)
job.commit()
