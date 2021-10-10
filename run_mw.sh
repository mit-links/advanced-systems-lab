#!/bin/sh
libs="src/main/resources/libs"

ant -f build.xml	# build with ant

# run mw
java -cp "dist/middleware.jar:${libs}/log4j-api-2.9.1.jar:${libs}/log4j-core-2.9.1.jar" ch.ethz.asltest.RunMW -l localhost -p 12345 -t 128 -s false -m localhost:11221 # localhost:11222 localhost:11223
