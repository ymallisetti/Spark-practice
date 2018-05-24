#!/bin/bash
start=`date +%s`
#############################################################################################################################
#    Version      Date              Modified By      					Description                                         #
#    --------     ----------        -------------    					-------------------                                 #
#    v0.1       2017-05-14         Dharmik Kachhia     			        dk parser setup                                    	#
#                                                                                                                           #
#############################################################################################################################

echo "------------------------------------------------------------------"
processed_dt=`date '+%Y-%m-%d'`
echo "INFO: Process date: $processed_dt"

function Runcommand
{
    "$@"
    local status=$?
    if [ $status -ne 0 ]; then
        echo "error with $@" >&2
		exit $status
    fi
    return $status
}

function MakeHDFSDir
{
	local status=0
	if hdfs dfs -test -d $1 ; then
		echo "Directory $1 exists."
		status=$(Runcommand hdfs dfs -chmod u+rwx $1)
	else
		status=$(Runcommand hdfs dfs -mkdir -p $1)
		echo "Directory $1 created."
    fi
    return $status
}

export HDFS_BASE_DIR=/dharmik/dkjob
export DIR_CONFIG=$HDFS_BASE_DIR/config
export DIR_INPUT=$HDFS_BASE_DIR/input
export TABLE_BASE_DIR=$HDFS_BASE_DIR/tables
export TABLE_JH=$TABLE_BASE_DIR/jh
export TABLE_EXCEPTIONS=$TABLE_BASE_DIR/exceptions

MakeHDFSDir $HDFS_BASE_DIR
MakeHDFSDir $DIR_CONFIG
MakeHDFSDir $DIR_INPUT
MakeHDFSDir $TABLE_BASE_DIR
MakeHDFSDir $TABLE_JH
MakeHDFSDir $TABLE_EXCEPTIONS

echo "-----------DK setup scripts ends----------------------------------"

