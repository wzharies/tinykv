#!/bin/bash
for ((i=1;i<=50;i++));
do
	echo "ROUND $i";
	make project3b >> out3b-1.log;
done
