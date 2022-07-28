#!/bin/bash
for ((i=1;i<=$1;i++));
do
        echo "ROUND $i";
        make project2c > ./out/out-2-$i.txt;
	make project3b > ./out/out-3-$i.txt;
done
