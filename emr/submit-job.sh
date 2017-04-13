#!/usr/bin/env bash

RELEASE_LABEL="emr-5.2.1"
MASTER_INSTANCE_TYPE="m1.medium"
NUM_SLAVES=4
SLAVE_INSTANCE_TYPE="m1.medium"

PAGE_RANK_NAME="Page Rank"

CONFIGURATION_LOCATION="file://environment.json"
BOOTSTRAP_LOCATION="<S3_PATH>/emr-bootstrap.sh"

PAGE_RANK_MAIN_CLASS="edu.gwu.big.data.PageRank"
PAGE_RANK_LOCATION="<S3_PATH_TO_JAR>"

LOG_URI="<S3_PATH>/logs"

EXECUTOR_MEMORY="1g"
DRIVER_MEMORY="1g"
NUM_CORES_PER_INSTANCE=2
CLUSTER_NAME="PageRank Cluster"
INPUT_PATH="<INSERT_PATH>"

/usr/bin/aws emr create-cluster --configurations "${CONFIGURATION_LOCATION}" --bootstrap-actions Path="${BOOTSTRAP_LOCATION}",Args=[],Name="Bootstrap" \
--name "${CLUSTER_NAME}" --release-label "${RELEASE_LABEL}" --applications Name=Spark \
--instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=${MASTER_INSTANCE_TYPE} InstanceGroupType=CORE,InstanceCount=${NUM_SLAVES},InstanceType=${SLAVE_INSTANCE_TYPE} \
--auto-terminate --enable-debugging --log-uri "${LOG_URI}" \
--steps Type=Spark,Name="${PAGE_RANK_NAME}",ActionOnFailure=TERMINATE_CLUSTER,Args=[--class,${PAGE_RANK_MAIN_CLASS},--deploy-mode,cluster,--master,yarn-cluster,${PAGE_RANK_LOCATION},--inputPath,${INPUT_PATH}]