#!/bin/bash

for((i=1;i<=$1;i++));
do
 echo out/out-${i}.txt
cat out/out-${i}.txt | grep FAIL
done
