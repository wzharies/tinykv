#!/bin/bash
for ((i=1;i<=10;i++));
do
	echo "ROUND $i";
	make project3b >> ./out/out3b-4.log;
done
