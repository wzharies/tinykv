#!/bin/bash
for ((i=1;i<=10;i++));
do
	echo "ROUND $i";
	make project2c >> ./out/out.txt;
done
