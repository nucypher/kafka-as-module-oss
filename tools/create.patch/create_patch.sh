#!/bin/bash

#
./build_project.sh

PATCH_DIR=tools/create.patch/kafka-patch

# clean up previous patch
if [ -d "$PATCH_DIR" ]
then
	rm -rf $PATCH_DIR/
fi

##
mkdir $PATCH_DIR
mkdir $PATCH_DIR/libs
mkdir $PATCH_DIR/libs/nucypher
mkdir $PATCH_DIR/libs/nucypher/lib
mkdir $PATCH_DIR/bin

#
cp -avr admin/build/libs/lib/*.jar $PATCH_DIR/libs/nucypher/lib/
cp -avr clients/build/libs/lib/*.jar $PATCH_DIR/libs/nucypher/lib/
cp -avr commons/build/libs/lib/*.jar $PATCH_DIR/libs/nucypher/lib/

#
cp -av admin/build/libs/*.jar $PATCH_DIR/libs/nucypher
cp -av clients/build/libs/*.jar $PATCH_DIR/libs/nucypher
cp -av commons/build/libs/*.jar $PATCH_DIR/libs/nucypher

# patched kafka-run-class.sh with bunch of NuCypher .jar's
cp tools/create.patch/kafka-run-class.sh $PATCH_DIR/bin


rm -rf $PATCH_DIR/libs/nucypher/lib/nucypher*.jar


# kafka patched jar
cp kafka/core/build/libs/kafka_2.10-1.0-SNAPSHOT.jar $PATCH_DIR/libs/


tar cvzf nucypher-patch-kafka_2.10-1.0-SNAPSHOT.tar.gz tools/create.patch/apply_patch.sh ./README.md tools/create.patch/kafka-patch/



