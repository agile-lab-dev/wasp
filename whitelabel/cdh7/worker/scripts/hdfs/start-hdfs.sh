#!/bin/bash
set -eax
hdfs --daemon start namenode 
hdfs --daemon start datanode 