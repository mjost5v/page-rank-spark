# Example project using PageRank

## Compiling
If running locally, use the local profile:
```bash
$ mvn clean install -P local
```

Otherwise, if compiling for EMR/Spark Cluster:
```bash
$ mvn clean install -P emr
```

## Running on EMR
Upload the standalone jar and the bootstraph script to S3
Use the submit-job script to submit via the AWS CLI