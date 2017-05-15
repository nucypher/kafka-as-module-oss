#!/bin/bash

# right now make patch for particular version of Kafka and Scala
# kafka_2.10-0.10.1.1.jar

KAFKA_DIR=$1

echo "Trying to patch a Kafka directory:"$KAFKA_DIR

# Check that Kafka directory is exist
if [ -d "$KAFKA_DIR" ]
then
   echo "Kafka directory is present. Trying to apply a patch!"
else
   echo "Kafka directory is missing. Unable to patch Kafka!"
   exit 1
fi


KAFKA_JAR=kafka_2.10-0.10.1.1.jar
PATCH_DIR=tools/create.patch/kafka-patch

# validate that correct version of Kafka and Scala are used
if [ -f "$KAFKA_DIR/libs/$KAFKA_JAR" ]
then
   echo "Kafka original '$KAFKA_JAR' file is present. Trying to apply a patch!"
else
   echo "Kafka original '$KAFKA_JAR' file is missing. Unable to patch Kafka!"
   exit 1
fi

# patched kafka-run-class.sh with bunch of NuCypher *.jar's
cp $PATCH_DIR/bin/kafka-run-class.sh $KAFKA_DIR/bin

# clean up previous patch
if [ -d "$KAFKA_DIR/lib/nucypher" ]
then
	rm -rf $KAFKA_DIR/lib/nucypher
fi

cp -avr $PATCH_DIR/libs/nucypher $KAFKA_DIR/libs/

# copy patched Kafka file
cp $PATCH_DIR/libs/kafka_2.10-1.0-SNAPSHOT.jar $KAFKA_DIR/libs/$KAFKA_JAR

