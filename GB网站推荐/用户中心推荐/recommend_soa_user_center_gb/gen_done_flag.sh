#!/bin/sh
#hdfs dfs -cp /user/oozie/done-flag/_SUCCESS     /user/oozie/oozie-apps/hw_bigdata_dw/coor-dependency-test/coord1/done-flag/$1/

hdfs dfs -mkdir $1/$2
