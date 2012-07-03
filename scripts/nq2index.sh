#!/bin/sh
#
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

# To allow the use of commons-configuration version 1.8 over Hadoop's version 1.6 we export HADOOP_USER_CLASSPATH_FIRST=true
# See https://issues.apache.org/jira/browse/MAPREDUCE-1938 and hadoop.apache.org/common/docs/r0.20.204.0/releasenotes.html
export HADOOP_USER_CLASSPATH_FIRST=true

HADOOP_NAME_NODE="localhost:9000"
DFS_ROOT_DIR="hdfs://${HADOOP_NAME_NODE}"
DFS_USER_DIR="${DFS_ROOT_DIR}/user/${USER}"
DFS_BUILD_DIR="${DFS_USER_DIR}/nq2index.${BUILD_NAME}"
LOCAL_BUILD_DIR="${HOME}/tmp/nq2index.${BUILD_NAME}"

PROJECT_JAR="../target/Glimmer-0.0.1-SNAPSHOT-jar-with-dependencies.jar"
GENERATE_INDEX_FILES="blacklist.txt,fixDataRSS.xsl,RDFa2RDFXML.xsl,t_namespaces.html"

COMPRESSION_CODEC="org.apache.hadoop.io.compress.BZip2Codec"
COMPRESSION_CODECS="org.apache.hadoop.io.compress.DefaultCodec,${COMPRESSION_CODEC}"

HASH_EXTENSION=".smap"

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
	local OUTPUT_DIR=${2}
	echo Processing tuples from file ${INPUT_FILENAME}...
	echo
	local CMD="${HADOOP_CMD} jar ${PROJECT_JAR} com.yahoo.glimmer.indexing.preprocessor.TuplesTool \
		-Dio.compression.codecs=${COMPRESSION_CODECS} \
		-Dmapred.map.tasks.speculative.execution=true \
		-Dmapred.child.java.opts=-Xmx800m \
		-Dmapred.job.map.memory.mb=2000 \
		-Dmapred.job.reduce.memory.mb=2000 \
		-Dmapred.output.compression.codec=${COMPRESSION_CODEC} \
		-Dmapred.output.compress=true \
		${INPUT_FILE} ${OUTPUT_DIR}"
	echo ${CMD}
	${CMD}
		
	local EXIT_CODE=$?
	if [ $EXIT_CODE -ne "0" ] ; then
		echo "TuplesTool exited with code $EXIT_CODE. exiting.."
		exit $EXIT_CODE
	fi	
}

function computeHashes () {
	FILES=$@
	echo
	echo Generating Hashes..
	echo
	# Generate Hashes for subjects, predicates and objects and all
	CMD="$HADOOP_CMD jar ${PROJECT_JAR} com.yahoo.glimmer.util.ComputeHashTool \
		-Dio.compression.codecs=${COMPRESSION_CODECS} \
		-su ${FILES}"
	echo ${CMD}; ${CMD}
		
	EXIT_CODE=$?
	if [ $EXIT_CODE -ne "0" ] ; then
		echo "Hash generation exited with code $EXIT_CODE. exiting.."
		exit $EXIT_CODE
	fi	
}

function getDocCount () {
    local BY_SUBJECT_DIR=${1}
	# The number of docs is the number of 'all' resources..
	# Note: It's really the number of subjects but as the MG4J docId is taken from the position in the all resources hash
	# MG4J expects that the docId be smaller that the number of docs.  Using the all resource count is simpler that using
	# the subject count and hash to get the docIds.  The effect is that the index contains empty docs and that the
	# Doc count it's accurate. Which may effect scoring in some cases.. 
	NUMBER_OF_DOCS=`${HADOOP_CMD} fs -cat ${BY_SUBJECT_DIR}/counts | grep all | cut -f 2`
	if [ -z "${NUMBER_OF_DOCS}" -o $? -ne "0" ] ; then
		echo "Failed to get the number of subjects. exiting.."
		exit 1
	fi
	echo "There are ${NUMBER_OF_DOCS} docs(subjects)."
}

function generateIndex () {
	BY_SUBJECT_DIR=${1}
	METHOD=${2}
	NUMBER_OF_DOCS=${3}
	SUBINDICES=${4}
	OUTPUT_DIR="${DFS_BUILD_DIR}/${METHOD}"
	
	echo
	echo "RUNING HADOOP INDEX BUILD FOR METHOD:" ${METHOD}
	echo
	
	${HADOOP_CMD} fs -test -e "${OUTPUT_DIR}"
	if [ $? -eq 0 ] ; then
		read -p "${OUTPUT_DIR} exists already! Delete and regenerate index, Continue using existing index or otherwise quit? (D/C)" -n 1 -r
		echo
		if [[ $REPLY =~ ^[Cc]$ ]] ; then
			echo Continuing with existing sub indexes in ${OUTPUT_DIR}
			return 0
		elif [[ ! $REPLY =~ ^[Dd]$ ]] ; then
			echo Exiting.
			exit 1
		fi
	
		echo "Deleting DFS index directory ${OUTPUT_DIR}.."
		${HADOOP_CMD} fs -rmr -skipTrash ${OUTPUT_DIR}
	fi
	 
	echo Generating index..
	CMD="${HADOOP_CMD} jar ${PROJECT_JAR} com.yahoo.glimmer.indexing.generator.TripleIndexGenerator \
		-Dio.compression.codecs=${COMPRESSION_CODECS} \
		-Dmapred.map.tasks.speculative.execution=true \
		-Dmapred.reduce.tasks=${SUBINDICES} \
		-Dmapred.child.java.opts=-Xmx800m \
		-Dmapred.job.map.memory.mb=2000 \
		-Dmapred.job.reduce.memory.mb=2000 \
		-files ${GENERATE_INDEX_FILES} \
		-m ${METHOD} -p ${BY_SUBJECT_DIR}/predicate.bz2 ${BY_SUBJECT_DIR}/part-r-?????/bysubject.bz2 $NUMBER_OF_DOCS ${OUTPUT_DIR} ${BY_SUBJECT_DIR}/all.map"
	echo ${CMD}
	${CMD}
		
	EXIT_CODE=$?
	if [ $EXIT_CODE -ne "0" ] ; then
		echo "TripleIndexGenerator MR job exited with code $EXIT_CODE. exiting.."
		exit $EXIT_CODE
	fi
	
	# Remove empty MR part-... files
	${HADOOP_CMD} fs -rmr -skipTrash "${OUTPUT_DIR}/part-*"
}

function getSubIndexes () {
	METHOD=${1}
	echo
	echo "COPYING SUB INDEXES TO LOCAL DISK FOR METHOD:" ${METHOD}
	echo
	
	MR_OUT_DIR="${LOCAL_BUILD_DIR}/${METHOD}/mrOut"
	if [ -d ${MR_OUT_DIR} ] ; then
		read -p "${MR_OUT_DIR} exists already! Overwrite, Continue using existing local files or otherwise quit? (O/C)" -n 1 -r
		echo
		if [[ $REPLY =~ ^[Cc]$ ]] ; then
			return 0
		elif [[ ! $REPLY =~ ^[Oo]$ ]] ; then
			echo ${MR_OUT_DIR} exists. Exiting..
			exit 1
		fi
		echo Deleting ${MR_OUT_DIR}
		rm -rf "${MR_OUT_DIR}"
	fi
	
	mkdir -p ${MR_OUT_DIR}
	CMD="${HADOOP_CMD} fs -copyToLocal ${DFS_BUILD_DIR}/${METHOD}/index/* ${MR_OUT_DIR}"
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
	MR_OUT_DIR="${LOCAL_BUILD_DIR}/${METHOD}/mrOut"
	echo
	echo "MERGING SUB INDEXES FOR METHOD:" ${METHOD}
	echo
	
	EXISTING_INDEX_FILES=`ls ${LOCAL_BUILD_DIR}/${METHOD}/*.index`
	if [ ! -z "${EXISTING_INDEX_FILES}" ] ; then
		read -p "Local .index files exist in ${LOCAL_BUILD_DIR}/${METHOD}! Continue(delete them) or otherwise quit? (C)" -n 1 -r
		echo
		if [[ ! $REPLY =~ ^[Cc]$ ]] ; then
			echo Exiting..
			exit 1
		fi
		echo Deleting old index files from ${LOCAL_BUILD_DIR}/${METHOD}...
		for FILE_EXT in frequencies index offsets positions posnumbits properties stats termmap terms ; do
			rm -f ${LOCAL_BUILD_DIR}/${METHOD}/*.${FILE_EXT}
		done
	fi
	
	MR_UUID_DIRS=`ls "${MR_OUT_DIR}"`
	echo "Map Reduce sub-index dirs are:"
	echo ${MR_UUID_DIRS[@]}
	echo
	
	INDEX_NAMES=`ls ${MR_OUT_DIR}/${MR_UUID_DIRS[0]} | awk '/\.index/{sub("\.index$","") ; print $0}'`
	echo "Index names are:"
	echo ${INDEX_NAMES[@]}
	echo
	
	for INDEX_NAME in ${INDEX_NAMES[@]}; do
		SUB_INDEXES=""
		for MR_UUID_DIR in ${MR_UUID_DIRS[@]}; do
			SUB_INDEXES="${SUB_INDEXES} ${MR_OUT_DIR}/${MR_UUID_DIR}/${INDEX_NAME}"
		done
		
		# When merging the alignment index there are no counts.
		NO_COUNTS_OPTIONS=""
		if [ "${INDEX_NAME}" == "alignment" ] ; then
			NO_COUNTS_OPTIONS="-cCOUNTS:NONE -cPOSITIONS:NONE"
		fi
		
		CMD="java -Xmx2G -cp ${PROJECT_JAR} it.unimi.dsi.mg4j.tool.Merge ${NO_COUNTS_OPTIONS} ${LOCAL_BUILD_DIR}/${METHOD}/${INDEX_NAME} ${SUB_INDEXES}"
		echo ${CMD}
		${CMD}
		
		EXIT_CODE=$?
		if [ $EXIT_CODE -ne 0 ] ; then
			echo "Merge of ${METHOD} returned and exit value of $EXIT_CODE. exiting.."
			exit $EXIT_CODE
		fi
		
		CMD="java -cp ${PROJECT_JAR} it.unimi.dsi.util.ImmutableExternalPrefixMap ${LOCAL_BUILD_DIR}/${METHOD}/${INDEX_NAME}.termmap -o ${LOCAL_BUILD_DIR}/${METHOD}/${INDEX_NAME}.terms"
		echo ${CMD}
		${CMD}
		
		EXIT_CODE=$?
		if [ $EXIT_CODE -ne 0 ] ; then
			echo "Creating terms map failed with value of $EXIT_CODE. exiting.."
			exit $EXIT_CODE
		fi
	done	
	echo rm -rf ${MR_OUT_DIR}
}

	
function generateDocSizes () {
	BY_SUBJECT_DIR=${1}
	METHOD=${2}
	NUMBER_OF_DOCS=${3}
	DFS_SIZES_DIR="${DFS_BUILD_DIR}/${METHOD}.sizes"
	
	echo
	echo GENERATING DOC SIZES..
	echo
	CMD="${HADOOP_CMD} jar ${PROJECT_JAR} com.yahoo.glimmer.indexing.DocSizesGenerator \
		-Dmapred.max.map.failures.percent=1 \
		-Dmapred.map.tasks.speculative.execution=true \
		-Dmapred.reduce.tasks=300 \
		-Dmapred.child.java.opts=-Xmx800m \
		-Dmapred.job.map.memory.mb=2000 \
		-D=mapred.job.reduce.memory.mb=2000 \
		-files ${GENERATE_INDEX_FILES} \
		-m ${METHOD} -p ${BY_SUBJECT_DIR}/predicate.bz2 ${BY_SUBJECT_DIR}/part-r-?????/bysubject.bz2 $NUMBER_OF_DOCS ${DFS_SIZES_DIR} ${BY_SUBJECT_DIR}/all.map"
	echo ${CMD}
	${CMD}
	EXIT_CODE=$?
	if [ $EXIT_CODE -ne 0 ] ; then
		echo "DocSizesGenerator failed with value of $EXIT_CODE. exiting.."
		exit $EXIT_CODE
	fi
	
	${HADOOP_CMD} fs -rmr -skipTrash "${DFS_SIZES_DIR}/*temp"
	${HADOOP_CMD} fs -copyToLocal "${DFS_SIZES_DIR}/*.sizes" "${LOCAL_BUILD_DIR}/${METHOD}"
}	

function buildCollection () {
	echo
	echo BUILDING COLLECTION..
	echo
	CMD="${HADOOP_CMD} jar ${PROJECT_JAR} com.yahoo.glimmer.indexing.BySubjectCollectionBuilder \
		-Dmapred.map.max.attempts=20 \
		-Dmapred.map.tasks.speculative.execution=false \
		-Dmapred.child.java.opts=-Xmx800m \
		-Dmapred.job.map.memory.mb=2000 \
		-Dmapred.job.reduce.memory.mb=2000 \
		-Dmapred.job.queue.name=$QUEUE \
		-Dmapred.min.split.size=8500000000 ${BY_SUBJECT_DIR}/part-r-?????/bysubject.bz2 ${DFS_BUILD_DIR}/collection/"
	echo ${CMD}
	${CMD}
	EXIT_CODE=$?
	if [ $EXIT_CODE -ne 0 ] ; then
		echo "BySubjectCollectionBuilder failed with value of $EXIT_CODE. exiting.."
		exit $EXIT_CODE
	fi
	
	${HADOOP_CMD} fs -copyToLocal "${DFS_BUILD_DIR}/collection" "${LOCAL_BUILD_DIR}"
}

#groupBySubject ${IN_FILE} ${DFS_BUILD_DIR}/bysubject
#computeHashes ${DFS_BUILD_DIR}/bysubject/all.bz2

getDocCount ${DFS_BUILD_DIR}/bysubject

#generateIndex ${DFS_BUILD_DIR}/bysubject horizontal ${NUMBER_OF_DOCS} ${SUBINDICES}
#getSubIndexes horizontal
#mergeSubIndexes horizontal

#generateIndex ${DFS_BUILD_DIR}/bysubject vertical ${NUMBER_OF_DOCS} ${SUBINDICES}
#getSubIndexes vertical
#mergeSubIndexes vertical


# These could be run in parallel with index generation.
generateDocSizes ${DFS_BUILD_DIR}/bysubject horizontal ${NUMBER_OF_DOCS}

exit 1
buildCollection

${HADOOP_CMD} fs -cat "${DFS_BUILD_DIR}/subjects.bz2/*.bz2" | ${BZCAT_CMD} > "${LOCAL_BUILD_DIR}/subjects.txt"
${HADOOP_CMD} fs -copyToLocal "${OUTPUT_NAMES[2]}/part-r-00000" "${LOCAL_BUILD_DIR}/predicates.txt"
${HADOOP_CMD} fs -copyToLocal "${DFS_BUILD_DIR}/subjects.mph" "${LOCAL_BUILD_DIR}"

echo Done. Index files are here ${LOCAL_BUILD_DIR}

