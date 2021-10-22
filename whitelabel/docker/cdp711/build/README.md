# docker-hadoop

This repository will be our training environment for the workshop "hadoop environment inside a docker image"

We are going to install open source versions instead of CDP versions, after the tutorial, where we will understand everything we need to do in order to make this "docker friendly" hadoop environment i will give you pointers to an implementation that will use a Cloudera Runtime Parcel to do the same things (Courtesy of Aleksandar)


## Components we need to install

* Hadoop prerequisites
* Hbase
* Spark
* Solr
* Kafka (we will get a zookeeper instance for free)

## What we need to take care of

* Image should be as slim as possible to improve the development experience (pull times, build times)
* The image should be built to maximize the caching opportunities, to improve our feedback loop
* We need a way to bust this cache if something is not working properly
* Containers will start up with dynamic hostnames (unless we tell the user to do something special) so we need to handle that
* RAM is scarce and we should make good use of that, we should keep those pesky heaps in check

## Caveat

* In order for this to work buildkit should be disabled (change that in the configs of your docker daemon)

## How should we organize this docker build

* Support scripts
* Installation scripts
* Runtime scripts
* Configuration templates

# How to Build

`docker build -t docker-hadoop .`

# How to Run

`docker run -p8088:8088 -p8042:8042 -p8192:8192 --rm -it docker-hadoop`

