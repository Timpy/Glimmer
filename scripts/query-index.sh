#!/bin/bash
#
# Copyright (c) 2012 Yahoo! Inc. All rights reserved.
# 
#  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
#  Unless required by applicable law or agreed to in writing, software distributed under the License is 
#  distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and limitations under the License.
#  See accompanying LICENSE file.
#

METHOD=${1}

BUILD_NAME="tmp"
if [ ! -z ${2} ] ; then
	BUILD_NAME=${2}
fi

LOCAL_BUILD_DIR="${HOME}/tmp/index-${BUILD_NAME}"
INDEX_DIR="${LOCAL_BUILD_DIR}/${METHOD}"


PROJECT_JAR="../target/Glimmer-0.0.1-SNAPSHOT-jar-with-dependencies.jar"

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

FILENAMES=`ls ${INDEX_DIR}/*.properties`
EXIT_CODE=$?
if [ $EXIT_CODE -ne "0" ] ; then
	exit $EXIT_CODE
fi

BASENAMES=
for FILENAME in ${FILENAMES} 
do 
  FIELDNAME=`echo ${FILENAME} | sed 's/.*\/\(.*\).properties$/\1/'`
  if [ ${FIELDNAME} != "alignment" ] ; then
  	BASENAMES=$BASENAMES' '${INDEX_DIR}/$FIELDNAME
  fi
done
echo $BASENAMES
${RLWRAP} java -Xmx3500m -cp $PROJECT_JAR it.unimi.di.mg4j.query.Query -n -v -T ${LOCAL_BUILD_DIR}/all.txt $BASENAMES
