#!/bin/bash

# 1
# Download submodules: Apache Kafka, bbs98-java
git submodule init
git submodule update

# 2
# Download gradle wrapper
gradle wrapper