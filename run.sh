#!/bin/bash
for ((i=1;i<=50;i++));
do
	echo "ROUND $i";
	make project2b >> out2b-2.log;
done
