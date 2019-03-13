#!/bin/sh
#hdfs dfs -cp /user/oozie/done-flag/_SUCCESS     /user/oozie/oozie-apps/hw_bigdata_dw/coor-dependency-test/coord1/done-flag/$1/

hdfs dfs -test -e  $1/$2

if [ $? -eq 0 ] ;then 
   echo 'the dir has exists' 
   hdfs dfs -rm -r  $1/$2
else
   echo 'the dir is not exist'
fi

