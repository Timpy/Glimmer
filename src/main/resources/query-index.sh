#!/bin/bash
#first param: query
#second param: method

METHOD=${1}

BUILD_NAME="tmp"
if [ ! -z ${2} ] ; then
	BUILD_NAME=${2}
fi

LOCAL_BUILD_DIR="${HOME}/tmp/nq2index.${BUILD_NAME}"
INDEX_DIR="${LOCAL_BUILD_DIR}/${METHOD}"


PROJECT_JAR="../Glimmer-0.0.1-SNAPSHOT-jar-with-dependencies.jar"

RLWRAP=$(which rlwrap)
if [ -z ${RLWRAP} ] ; then
	echo "No rlwrap executable found in \$PATH.  Command line will not be editable."
else
	echo "Using rlwrap ${RLWRAP}"
fi

if [ ! -d ${INDEX_DIR} ] ; then
	echo No index directory found at ${INDEX_DIR}
	exit -1;
fi

FILENAMES=`ls ${INDEX_DIR}/*.index`
EXIT_CODE=$?
if [ $EXIT_CODE -ne "0" ] ; then
	exit $EXIT_CODE
fi

BASENAMES=
for FILENAME in ${FILENAMES} 
do 
  FIELDNAME=`echo ${FILENAME} | sed 's/.*\/\(.*\).index$/\1/'`
  if [ ${FIELDNAME} != "alignment" ] ; then
  	BASENAMES=$BASENAMES' '${INDEX_DIR}/$FIELDNAME
  fi
done
echo $BASENAMES
${RLWRAP} java -Xmx4G -cp $PROJECT_JAR it.unimi.dsi.mg4j.query.Query -n -v -T ${LOCAL_BUILD_DIR}/subjects.txt $BASENAMES
