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

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://nnamdifirstbucket/stediproject/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerTrusted_node1",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1696025811864 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://nnamdifirstbucket/stediproject/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerTrusted_node1696025811864",
)

# Script generated for node Join
Join_node1696025868062 = Join.apply(
    frame1=StepTrainerTrusted_node1,
    frame2=AccelerometerTrusted_node1696025811864,
    keys1=["sensorReadingTime"],
    keys2=["timeStamp"],
    transformation_ctx="Join_node1696025868062",
)

# Script generated for node Machine Learning Curated
MachineLearningCurated_node2 = glueContext.getSink(
    path="s3://nnamdifirstbucket/stediproject/machine_learning/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="MachineLearningCurated_node2",
)
MachineLearningCurated_node2.setCatalogInfo(
    catalogDatabase="stedi_project", catalogTableName="machine_learning_curated"
)
MachineLearningCurated_node2.setFormat("json")
MachineLearningCurated_node2.writeFrame(Join_node1696025868062)
job.commit()
