# UW Big Data Certificate
## 210 Data Engineering Introduction

### Final Project via YouTube

[![Final Project YouTube Link](http://img.youtube.com/vi/TS_mAIuJbIY/0.jpg)](http://www.youtube.com/watch?v=TS_mAIuJbIY)

#### Related links
- AWS EMR / Spark
  - [Amazon EMR Product](https://aws.amazon.com/emr/): General EMR product information.
  - [Spark in EMR](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-configure.html): Information and configurations for running Spark in Amazon EMR.
  - [How To - Set up Sagemaker Notebook with EMR](https://aws.amazon.com/blogs/machine-learning/build-amazon-sagemaker-notebooks-backed-by-spark-in-amazon-emr/): Instructions for configuring an EMR cluster to enable connections from a Sagemaker notebook.
  - [AWS Introduces EMR Notebooks](https://aws.amazon.com/about-aws/whats-new/2018/11/introducing-emr-notebooks-a-managed-analytics-environment-based-on-jupyter-notebooks/): Announcement introducing launching notebooks directly from the EMR console.
  - [EMR Security](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-managed-notebooks-security-groups.html): Article explaining how to manage security groups for EMR.
- Working with JSON in Spark
  - [JSON in Spark](https://spark.apache.org/docs/latest/sql-data-sources-json.html): General instructions for loading and interfacing with JSON in Spark.
  - [Stack Overflow: Flattening Rows in Spark](https://stackoverflow.com/questions/32906613/flattening-rows-in-spark): Conversation about how to deal with nested JSON in Spark.
  - [Explode Spark Function](https://spark.apache.org/docs/2.3.0/api/sql/index.html#explode): Used for flattening (aka exploding) the elements within a nested structure.
- Datasets
  - Chicago Building Permits
    - [Chicago Data Portal Summary](https://data.cityofchicago.org/Buildings/Building-Permits/ydr8-5enu)
    - [GEOJSON Download Link (1.8GB)](https://data.cityofchicago.org/api/geospatial/ydr8-5enu?method=export&format=GeoJSON)
- Analysis
  - [Notebook](notebook.ipynb)

#### Terminal commands

To copy data from Chicago Data Portal then move to S3.
```
wget -O chicagopermit.json "https://data.cityofchicago.org/api/geospatial/ydr8-5enu?method=export&format=GeoJSON"

aws s3 cp chicagopermit.json s3://$bucket/chicagopermit.json
```
