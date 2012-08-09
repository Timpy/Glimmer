#!/bin/sh
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

INPUT_ARG=${1}
if [ -z ${INPUT_ARG} ] ; then
	echo Usage: "${0} <tuple file on local disk or HDFS> [build name] [no. sub indices]"
	exit 1
fi

BUILD_NAME="tmp"
if [ ! -z ${2} ] ; then
	BUILD_NAME=${2}
fi

SUBINDICES=20
if [ ! -z ${3} ] ; then
	SUBINDICES=${3}
fi

# Set to "-C" to exclude context from processing. 
EXCLUDE_CONTEXTS=""
# Set PrepTool's -s -p -o -c -a options here to exclude tuples not matching the given regexes.
PREP_FILTERS=""

# Number of predicates to use when building vertical indexes.  
# The occurrences of predicates found in the source tuples are counted and then sorted by occurrence count.
# This limits the resulting list to the top N predicates.
N_VERTICAL_PREDICATES=200

# To allow the use of commons-configuration version 1.8 over Hadoop's version 1.6 we export HADOOP_USER_CLASSPATH_FIRST=true
# See https://issues.apache.org/jira/browse/MAPREDUCE-1938 and hadoop.apache.org/common/docs/r0.20.204.0/releasenotes.html
export HADOOP_USER_CLASSPATH_FIRST=true

#HADOOP_NAME_NODE="localhost:9000"
HADOOP_NAME_NODE=""
DFS_ROOT_DIR="hdfs://${HADOOP_NAME_NODE}"
DFS_USER_DIR="${DFS_ROOT_DIR}/user/${USER}"
DFS_BUILD_DIR="${DFS_USER_DIR}/index-${BUILD_NAME}"
LOCAL_BUILD_DIR="${HOME}/tmp/index-${BUILD_NAME}"

PROJECT_JAR="../target/Glimmer-0.0.1-SNAPSHOT-jar-with-dependencies.jar"
HADOOP_CACHE_FILES="../target/classes/blacklist.txt"

COMPRESSION_CODEC="org.apache.hadoop.io.compress.BZip2Codec"
COMPRESSION_CODECS="org.apache.hadoop.io.compress.DefaultCodec,${COMPRESSION_CODEC}"

HASH_EXTENSION=".smap"

INDEX_FILE_EXTENSIONS="frequencies index offsets positions posnumbits properties stats termmap terms"

if [ ! -f ${PROJECT_JAR} ] ; then
	echo "Projects jar file missing!! ${PROJECT_JAR}"
	exit 1
fi

HADOOP_CMD=`which hadoop`
if [ -z ${HADOOP_CMD} ] ; then
	echo "Can't find the hadoop command."
	exit 1
fi

BZCAT_CMD=`which bzcat`
if [ -z ${BZCAT_CMD} ] ; then
	echo "Can't find the bzcat command."
	exit 1
fi

${HADOOP_CMD} dfs -test -d ${DFS_BUILD_DIR}
if [ $? -ne 0 ] ; then
	echo "Creating DFS build directory ${DFS_BUILD_DIR}.."
	${HADOOP_CMD} dfs -mkdir ${DFS_BUILD_DIR}
	if [ $? -ne 0 ] ; then
		echo "Failed to create build directory ${DFS_BUILD_DIR} in DFS."
		exit 1
	fi
else
	read -p "Build dir ${DFS_BUILD_DIR} already exists in DFS. Continue anyway? (Y)" -n 1 -r
	echo
	if [[ ! $REPLY =~ ^[Yy]$ ]] ; then
		exit 1
	fi
fi

if [ ! -d ${LOCAL_BUILD_DIR} ] ; then
	echo "Creating local build directory ${LOCAL_BUILD_DIR}.."
	mkdir ${LOCAL_BUILD_DIR}
	if [ $? -ne 0 ] ; then
		echo "Failed to create local build directory ${LOCAL_BUILD_DIR}."
		exit 1
	fi
else
	read -p "Local build dir ${LOCAL_BUILD_DIR} already exists. Continue anyway? (Y)" -n 1 -r
	echo
	if [[ ! $REPLY =~ ^[Yy]$ ]] ; then
		exit 1
	fi
fi

# Is INPUT_ARG a local or in HDFS file?
IN_FILE=unset
if [[ ${INPUT_ARG} == hdfs:* ]] ; then
	${HADOOP_CMD} fs -test -e "${INPUT_ARG}"
	if [ $? -ne 0 ] ; then
		echo "Can't find file ${INPUT_ARG} on cluster!"
		exit 1
	fi
	IN_FILE=${INPUT_ARG}
	echo Using file ${IN_FILE} on cluster as input..
elif [ -f "${INPUT_ARG}" ] ; then
	echo "Uploading local file ${INPUT_ARG} to cluster.."
	IN_FILE="${DFS_BUILD_DIR}"/$(basename "${INPUT_ARG}")
	${HADOOP_CMD} fs -test -e "${IN_FILE}"
	if [ $? -eq 0 ] ; then
		read -p "File ${INPUT_ARG} already exists on cluster as ${IN_FILE}. Overwrite, Continue(using file on cluster) or otherwise quit? (O/C)" -n 1 -r
		echo
		if [[ $REPLY =~ ^[Cc]$ ]] ; then
			INPUT_ARG=""
		elif [[ ! $REPLY =~ ^[Oo]$ ]] ; then
			exit 1
		fi
	fi
	
	if [ ! -z ${INPUT_ARG} ] ; then
		${HADOOP_CMD} fs -put "${INPUT_ARG}" "${DFS_BUILD_DIR}"
		if [ $? -ne 0 ] ; then
			echo "Failed to upload input file ${INPUT_ARG} to ${IN_FILE}"
			exit 1
		fi
		echo "Uploaded ${INPUT_ARG} to ${IN_FILE}"
	fi	
else
	echo "${INPUT_ARG} not found."
	echo "Give either a local file to upload or the full URL of a file on the cluster."
	exit 1
fi

function groupBySubject () {
	local INPUT_FILE=${1}
	local PREP_DIR=${2}
	local REDUCER_TASKS=${3}
	echo Processing tuples from file ${INPUT_FILE}...
	echo
	local CMD="${HADOOP_CMD} jar ${PROJECT_JAR} com.yahoo.glimmer.indexing.preprocessor.PrepTool \
		-Dio.compression.codecs=${COMPRESSION_CODECS} \
		-Dmapred.map.tasks.speculative.execution=true \
		-Dmapred.child.java.opts=-Xmx800m \
		-Dmapred.job.map.memory.mb=2000 \
		-Dmapred.job.reduce.memory.mb=2000 \
		-Dmapred.reduce.tasks=1 \
		-Dmapred.output.compression.codec=${COMPRESSION_CODEC} \
		-Dmapred.output.compress=false \
		-Dmapred.job.queue.name=${QUEUE} \
		${PREP_FILTERS} ${EXCLUDE_CONTEXTS} ${INPUT_FILE} ${PREP_DIR}"
	echo ${CMD}
	${CMD}
		
	local EXIT_CODE=$?
	if [ $EXIT_CODE -ne "0" ] ; then
		echo "PrepTool exited with code $EXIT_CODE. exiting.."
		exit $EXIT_CODE
	fi
	
	local CMD="${HADOOP_CMD} fs -mv ${PREP_DIR}/part-r-00000/* ${PREP_DIR}"
	echo ${CMD}
	${CMD}
	
	${HADOOP_CMD} fs -cat ${PREP_DIR}/predicates | sort -nr | cut -f 2 | head -${N_VERTICAL_PREDICATES} > ${LOCAL_BUILD_DIR}/topPredicates
	${HADOOP_CMD} fs -put ${LOCAL_BUILD_DIR}/topPredicates ${PREP_DIR}
	
	local CMD="${HADOOP_CMD} jar ${PROJECT_JAR} com.yahoo.glimmer.util.MergeSortTool \
		-Dio.compression.codecs=${COMPRESSION_CODECS} \
		-Dmapred.child.java.opts=-Xmx800m \
		-Dmapred.output.compression.codec=${COMPRESSION_CODEC} \
		-Dmapred.output.compress=true \
		-i ${PREP_DIR}/part-r-?????/all.bz2 -o ${PREP_DIR}/all.bz2 -c all.count"
#	echo ${CMD}
#	${CMD}
	
	local CMD="${HADOOP_CMD} jar ${PROJECT_JAR} com.yahoo.glimmer.util.MergeSortTool \
		-Dio.compression.codecs=${COMPRESSION_CODECS} \
		-Dmapred.child.java.opts=-Xmx800m \
		-Dmapred.output.compression.codec=${COMPRESSION_CODEC} \
		-Dmapred.output.compress=true \
		-i ${PREP_DIR}/part-r-?????/predicates.bz2 -o ${PREP_DIR}/predicates.bz2 -c predicates.count"
#	echo ${CMD}
#	${CMD}
}

function computeHashes () {
	FILES=$@
	echo
	echo Generating Hashes..
	echo "		If you get out of disk space errors you need more space in /tmp for ChunkedHashStore... files"
	echo "		If you get out of heap errors try setting hadoop's HADOOP_HEAPSIZE or HADOOP_CLIENT_OPTS=\"-Xmx..."
	echo
	# Generate Hashes for subjects, predicates and objects and all
	CMD="$HADOOP_CMD jar ${PROJECT_JAR} com.yahoo.glimmer.util.ComputeHashTool \
		-Dio.compression.codecs=${COMPRESSION_CODECS} \
		-sui ${FILES}"
	echo ${CMD}; ${CMD}
		
	EXIT_CODE=$?
	if [ $EXIT_CODE -ne "0" ] ; then
		echo "Hash generation exited with code $EXIT_CODE. exiting.."
		exit $EXIT_CODE
	fi	
}

function getDocCount () {
    local PREP_DIR=${1}
	# The number of docs is the number of 'all' resources..
	# Note: It's really the number of subjects but as the MG4J docId is taken from the position in the all resources hash
	# MG4J expects that the docId be smaller that the number of docs.  Using the all resource count is simpler that using
	# the subject count and hash to get the docIds.  The effect is that the index contains empty docs and that the
	# Doc count it's accurate. Which may effect scoring in some cases.. 
	NUMBER_OF_DOCS=`${HADOOP_CMD} fs -cat ${PREP_DIR}/all.mapinfo | grep size | cut -f 2`
	if [ -z "${NUMBER_OF_DOCS}" -o $? -ne "0" ] ; then
		echo "Failed to get the number of subjects. exiting.."
		exit 1
	fi
	echo "There are ${NUMBER_OF_DOCS} docs(subjects)."
}

function generateIndex () {
	PREP_DIR=${1}
	METHOD=${2}
	NUMBER_OF_DOCS=${3}
	SUBINDICES=${4}
	METHOD_DIR="${DFS_BUILD_DIR}/${METHOD}"
	
	echo
	echo "RUNING HADOOP INDEX BUILD FOR METHOD:" ${METHOD}
	echo "		When building the vertical indexes a lot of files are created and could possibly exceed your HDFS file count quota."
	echo "		The number of files for the vertical index is roughly equal to:"
	echo "			number of predicates * 11 * number of sub indicies" 
	echo
	
	${HADOOP_CMD} fs -test -e "${METHOD_DIR}"
	if [ $? -eq 0 ] ; then
		read -p "${METHOD_DIR} exists already! Delete and regenerate indexes, Continue using existing indexes or otherwise quit? (D/C)" -n 1 -r
		echo
		if [[ $REPLY =~ ^[Cc]$ ]] ; then
			echo Continuing with existing indexes in ${METHOD_DIR}
			return 0
		elif [[ ! $REPLY =~ ^[Dd]$ ]] ; then
			echo Exiting.
			exit 1
		fi
	
		echo "Deleting DFS indexes in directory ${METHOD_DIR}.."
		${HADOOP_CMD} fs -rmr -skipTrash ${METHOD_DIR}
	fi
	
	echo Generating index..
	local CMD="${HADOOP_CMD} jar ${PROJECT_JAR} com.yahoo.glimmer.indexing.generator.TripleIndexGenerator \
		-Dio.compression.codecs=${COMPRESSION_CODECS} \
		-Dmapred.map.tasks.speculative.execution=true \
		-Dmapred.reduce.tasks=${SUBINDICES} \
		-Dmapred.child.java.opts=-Xmx900m \
		-Dmapred.job.map.memory.mb=2000 \
		-Dmapred.job.reduce.memory.mb=2000 \
		-Dio.sort.mb=128 \
		-Dmapred.job.queue.name=${QUEUE} \
		-files ${HADOOP_CACHE_FILES} \
		-m ${METHOD} ${EXCLUDE_CONTEXTS} -p ${PREP_DIR}/topPredicates ${PREP_DIR}/bySubject $NUMBER_OF_DOCS ${METHOD_DIR} ${PREP_DIR}/all.map"
	echo ${CMD}
	${CMD}
	
	EXIT_CODE=$?
	if [ $EXIT_CODE -ne "0" ] ; then
		echo "TripleIndexGenerator MR job exited with code $EXIT_CODE. exiting.."
		exit $EXIT_CODE
	fi
}

function getSubIndexes () {
	METHOD=${1}
	echo
	echo "COPYING SUB INDEXES TO LOCAL DISK FOR METHOD:" ${METHOD}
	echo
	
	INDEX_DIR="${LOCAL_BUILD_DIR}/${METHOD}"
	if [ -d ${INDEX_DIR} ] ; then
		read -p "${INDEX_DIR} exists already! Overwrite, Continue using existing local files or otherwise quit? (O/C)" -n 1 -r
		echo
		if [[ $REPLY =~ ^[Cc]$ ]] ; then
			return 0
		elif [[ ! $REPLY =~ ^[Oo]$ ]] ; then
			echo ${INDEX_DIR} exists. Exiting..
			exit 1
		fi
		echo Deleting ${INDEX_DIR}
		rm -rf "${INDEX_DIR}"
	fi
	
	mkdir -p ${INDEX_DIR}
	CMD="${HADOOP_CMD} fs -copyToLocal ${DFS_BUILD_DIR}/${METHOD}/part-r-????? ${INDEX_DIR}"
	echo ${CMD}
	${CMD}
	
	EXIT_CODE=$?
	if [ $EXIT_CODE -ne "0" ] ; then
		echo "Failed to copy sub indexes from cluster. Exited with code $EXIT_CODE. exiting.."
		exit $EXIT_CODE
	fi
}

function mergeSubIndexes() {
	METHOD=${1}
	INDEX_DIR="${LOCAL_BUILD_DIR}/${METHOD}"
	echo
	echo "MERGING SUB INDEXES FOR METHOD:" ${METHOD}
	echo
	
	if [ -e "${INDEX_DIR}/*.index" ] ; then
		read -p "Local .index files exist in ${INDEX_DIR}! Continue(delete them) or otherwise quit? (C)" -n 1 -r
		echo
		if [[ ! $REPLY =~ ^[Cc]$ ]] ; then
			echo Exiting..
			exit 1
		fi
		echo Deleting old index files from ${INDEX_DIR}...
		for FILE_EXT in ${INDEX_FILE_EXTENSIONS} ; do
			rm -f ${INDEX_DIR}/*.${FILE_EXT}
		done
	fi
	
	PART_DIRS=(`ls -1d ${INDEX_DIR}/part-r-?????`)
	echo "Map Reduce part dirs are:"
	echo ${PART_DIRS[@]}
	echo
	
	INDEX_NAMES=`ls ${PART_DIRS[0]} | awk '/\.index/{sub(".index$","") ; print $0}'`
	echo "Index names are:"
	echo ${INDEX_NAMES[@]}
	echo
	
	for INDEX_NAME in ${INDEX_NAMES[@]}; do
		SUB_INDEXES=""
		for PART_DIR in ${PART_DIRS[@]}; do
			SUB_INDEXES="${SUB_INDEXES} ${PART_DIR}/${INDEX_NAME}"
		done
		
		# When merging the alignment index there are no counts.
		NO_COUNTS_OPTIONS=""
		if [ "${INDEX_NAME}" == "alignment" ] ; then
			NO_COUNTS_OPTIONS="-cCOUNTS:NONE -cPOSITIONS:NONE"
		fi
		
		CMD="java -Xmx2G -cp ${PROJECT_JAR} it.unimi.di.mg4j.tool.Merge --interleaved ${NO_COUNTS_OPTIONS} ${INDEX_DIR}/${INDEX_NAME} ${SUB_INDEXES}"
		echo ${CMD}
		${CMD}
		
		EXIT_CODE=$?
		if [ $EXIT_CODE -ne 0 ] ; then
			echo "Merge of ${METHOD} returned and exit value of $EXIT_CODE. exiting.."
			exit $EXIT_CODE
		fi
		
		echo "Removing part files for index ${INDEX_NAME}"
		for PART_DIR in ${PART_DIRS[@]}; do
			rm ${PART_DIR}/${INDEX_NAME}.*
		done
		
		CMD="java -cp ${PROJECT_JAR} it.unimi.dsi.util.ImmutableExternalPrefixMap ${INDEX_DIR}/${INDEX_NAME}.termmap -o ${INDEX_DIR}/${INDEX_NAME}.terms"
		echo ${CMD}
		${CMD}
		
		EXIT_CODE=$?
		if [ $EXIT_CODE -ne 0 ] ; then
			echo "Creating terms map failed with value of $EXIT_CODE. exiting.."
			exit $EXIT_CODE
		fi
	done	
	rm -rf ${INDEX_DIR}/part-r-?????
}

	
function generateDocSizes () {
	PREP_DIR=${1}
	METHOD=${2}
	NUMBER_OF_DOCS=${3}
	
	echo
	echo GENERATING DOC SIZES..
	echo
	
	DFS_SIZES_DIR="${DFS_BUILD_DIR}/${METHOD}.sizes"
	REDUCE_TASKS=$(( 1 + ${NUMBER_OF_DOCS} / 1000000 ))
	
	CMD="${HADOOP_CMD} jar ${PROJECT_JAR} com.yahoo.glimmer.indexing.DocSizesGenerator \
		-Dmapred.max.map.failures.percent=1 \
		-Dmapred.map.tasks.speculative.execution=true \
		-Dmapred.reduce.tasks=${REDUCE_TASKS} \
		-Dmapred.child.java.opts=-Xmx800m \
		-Dmapred.job.map.memory.mb=2000 \
		-D=mapred.job.reduce.memory.mb=2000 \
		-Dmapred.job.queue.name=${QUEUE} \
		-files ${HADOOP_CACHE_FILES} \
		-m ${METHOD} -p ${PREP_DIR}/predicate ${PREP_DIR}/bySubject $NUMBER_OF_DOCS ${DFS_SIZES_DIR} ${PREP_DIR}/all.map"
	echo ${CMD}
	${CMD}
	EXIT_CODE=$?
	
	${HADOOP_CMD} fs -rmr -skipTrash "${DFS_SIZES_DIR}/*-temp"
	
	if [ $EXIT_CODE -ne 0 ] ; then
		echo "DocSizesGenerator failed with value of $EXIT_CODE. exiting.."
		exit $EXIT_CODE
	fi
	
	${HADOOP_CMD} fs -copyToLocal "${DFS_SIZES_DIR}/*.sizes" "${LOCAL_BUILD_DIR}/${METHOD}"
}	

function buildCollection () {
	PREP_DIR=${1}
	COLLECTION_DIR="${DFS_BUILD_DIR}/collection"
	
	echo
	echo BUILDING COLLECTION in ${COLLECTION_DIR}
	echo
	
	${HADOOP_CMD} fs -test -e "${COLLECTION_DIR}"
	if [ $? -eq 0 ] ; then
		read -p "${COLLECTION_DIR} exists. Delete it or otherwise quit? (D)" -n 1 -r
		echo
		if [[ ! $REPLY =~ ^[Dd]$ ]] ; then
			echo Exiting..
			exit 1
		fi
		echo Deleting ${COLLECTION_DIR}...
		${HADOOP_CMD} fs -rmr -skipTrash ${COLLECTION_DIR}
	fi
	
	
	CMD="${HADOOP_CMD} jar ${PROJECT_JAR} com.yahoo.glimmer.indexing.BySubjectCollectionBuilder \
		-Dmapred.map.max.attempts=20 \
		-Dmapred.map.tasks.speculative.execution=false \
		-Dmapred.child.java.opts=-Xmx800m \
		-Dmapred.job.map.memory.mb=2000 \
		-Dmapred.job.reduce.memory.mb=2000 \
		-Dmapred.job.queue.name=${QUEUE} \
		-Dmapred.min.split.size=8500000000 ${PREP_DIR}/bySubject ${COLLECTION_DIR}"
	echo ${CMD}
	${CMD}
	EXIT_CODE=$?
	if [ $EXIT_CODE -ne 0 ] ; then
		echo "BySubjectCollectionBuilder failed with value of $EXIT_CODE. exiting.."
		exit $EXIT_CODE
	fi
	
	${HADOOP_CMD} fs -copyToLocal "${DFS_BUILD_DIR}/collection" "${LOCAL_BUILD_DIR}"
}

groupBySubject ${IN_FILE} ${DFS_BUILD_DIR}/prep ${SUBINDICES}
computeHashes ${DFS_BUILD_DIR}/prep/all

getDocCount ${DFS_BUILD_DIR}/prep

generateIndex ${DFS_BUILD_DIR}/prep horizontal ${NUMBER_OF_DOCS} ${SUBINDICES}
getSubIndexes horizontal
mergeSubIndexes horizontal

generateIndex ${DFS_BUILD_DIR}/prep vertical ${NUMBER_OF_DOCS} ${SUBINDICES}
getSubIndexes vertical
mergeSubIndexes vertical

# These could be run in parallel with index generation.
generateDocSizes ${DFS_BUILD_DIR}/prep horizontal ${NUMBER_OF_DOCS}

buildCollection ${DFS_BUILD_DIR}/prep

#${HADOOP_CMD} fs -cat "${DFS_BUILD_DIR}/prep/all.bz2" | ${BZCAT_CMD} > "${LOCAL_BUILD_DIR}/all.txt"
${HADOOP_CMD} fs -copyToLocal "${DFS_BUILD_DIR}/prep/all" "${LOCAL_BUILD_DIR}/all.txt"
${HADOOP_CMD} fs -copyToLocal "${DFS_BUILD_DIR}/prep/all.smap" "${LOCAL_BUILD_DIR}"
#${HADOOP_CMD} fs -cat "${DFS_BUILD_DIR}/prep/predicates.bz2" | ${BZCAT_CMD} > "${LOCAL_BUILD_DIR}/predicates.txt"
${HADOOP_CMD} fs -copyToLocal "${DFS_BUILD_DIR}/prep/topPredicates" "${LOCAL_BUILD_DIR}/predicates.txt"

echo Done. Index files are here ${LOCAL_BUILD_DIR}

