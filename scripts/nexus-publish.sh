#!/usr/bin/env bash

ssh -L 8081:localhost:8081  root@server01.cluster01.atscom.it ~& sbt clean publish