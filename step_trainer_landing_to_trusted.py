import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Curated
CustomerCurated_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://nnamdifirstbucket/stediproject/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="CustomerCurated_node1",
)

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1696022378135 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://nnamdifirstbucket/stediproject/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanding_node1696022378135",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1696022912153 = ApplyMapping.apply(
    frame=CustomerCurated_node1,
    mappings=[
        ("serialNumber", "string", "right_serialNumber", "string"),
        ("birthDay", "string", "birthDay", "string"),
        ("shareWithPublicAsOfDate", "long", "shareWithPublicAsOfDate", "long"),
        ("shareWithResearchAsOfDate", "long", "shareWithResearchAsOfDate", "long"),
        ("registrationDate", "long", "registrationDate", "long"),
        ("customerName", "string", "customerName", "string"),
        ("email", "string", "email", "string"),
        ("lastUpdateDate", "long", "lastUpdateDate", "long"),
        ("phone", "string", "phone", "string"),
        ("shareWithFriendsAsOfDate", "long", "shareWithFriendsAsOfDate", "long"),
    ],
    transformation_ctx="RenamedkeysforJoin_node1696022912153",
)

# Script generated for node Join
Join_node1696022848508 = Join.apply(
    frame1=RenamedkeysforJoin_node1696022912153,
    frame2=StepTrainerLanding_node1696022378135,
    keys1=["right_serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="Join_node1696022848508",
)

# Script generated for node Drop Fields
DropFields_node1696022991664 = DropFields.apply(
    frame=Join_node1696022848508,
    paths=[
        "right_serialNumber",
        "birthDay",
        "shareWithPublicAsOfDate",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "customerName",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFields_node1696022991664",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node2 = glueContext.getSink(
    path="s3://nnamdifirstbucket/stediproject/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="StepTrainerTrusted_node2",
)
StepTrainerTrusted_node2.setCatalogInfo(
    catalogDatabase="stedi_project", catalogTableName="step_trainer_trusted"
)
StepTrainerTrusted_node2.setFormat("json")
StepTrainerTrusted_node2.writeFrame(DropFields_node1696022991664)
job.commit()
