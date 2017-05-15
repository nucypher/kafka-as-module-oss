#!/bin/bash

# totally build project with all submodules
#./gradlew clean :commons:build :admin:build :clients:build :examples:build -x test
#&&
# extra keys forced by kafka build
./gradlew clean build -x test -x checkstyleMain -x checkstyleTest