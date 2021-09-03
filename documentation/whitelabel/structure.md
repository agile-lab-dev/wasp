## Whitelabel project structure

Inside of the whitelabel/docker directory we can find multiple different builds of WASP.
These builds are differentiated by the Hadoop or CDH(Cloudera Distribution Hadoop) version.

The "default" build/flavor of WASP should be tested against the vanilla-hadoop2 docker image.
 
```
whitelabel
├─── docker
│       ├─── vanilla-hadoop2
│       ├─── cdh5
│       ├─── cdh6
│       └─── cdh7
│   
├─── consumers-rt
│       └─── src
├─── consumers-spark
│       └─── src
├─── master
│       └─── src
├─── models
│       └─── src/main/scala/it/agilelab/bigdata/wasp/whitelabel/models
│                       ├─── example
│                       └─── test
├─── single-node
│       └─── cdh7
└─── producers
        └─── src/main/scala/it/agilelab/bigdata/wasp
                        ├─── producers/metrics/kafka
                        └─── whitelabel/producers
```
