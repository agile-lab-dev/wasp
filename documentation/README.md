![WASP_logo](icons/WASP_logo.jpg)

# WASP - Wide Analytics Streaming Platform


### General

#### Overview
WASP is a framework to build complex real time big data applications.
It relies on a kind of Kappa/Lambda architecture mainly leveraging Kafka and Spark.

If you need to ingest huge amount of heterogeneous data and analyze them through complex pipelines, this is the framework for you.
If you need a point and click product, this is not the tool for you.

WASP is a big data framework that allows you to not waste time with devops architectures and integrating different components. WASP lets you focus on your data, business logic and algorithms, without worrying about tipical big data problems like:

- at least once or exactly once delivery
- periodically training a machine learning model
- publishing your results in real time, to be reactive
- applying schemas to unstructured data
- feeding different datastores from the same data flow in a safe way
- etc.

#### WASP in the wild
WASP has been added to Cloudera Solution Gallery as an Open Source tool to simplify streaming workflows.

**You can see it [here](https://www.cloudera.com/solutions/gallery/agilelab-wide-streaming-analytics-platform.html)!**


#### Background
Handling huge streams of data in near real time is a hard task. So we want to build a reference architecture to speed up fast data application development and to avoid common mistakes about fault tolerance and reliability.
Kafka is the central pillar of the architecture and helps to handle streams in the correct way. We have been inspired by the Kappa architecture definition.