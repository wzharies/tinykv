#!/bin/bash
start_time=$(date +%s)
[ -e /tmp/fd1 ] || mkfifo /tmp/fd1
exec 3<>/tmp/fd1
rm -rf /tmp/fd1
# 同时执行 3 个线程
for ((i = 1; i <= $1; i++)); do
  echo >&3
done

for ((i = 1; i <= $2; i++)); do
  read -u3
  {
    echo "ROUND $i"
    make project1 > ./out/out-1--$i.txt;
    make project2 > ./out/out-2--$i.txt;
    make project3 > ./out/out-3-$i.txt;
    make project4 > ./out/out-4-$i.txt;
    echo >&3
  } &
done
wait

stop_time=$(date +%s)

echo "TIME:$(expr $stop_time - $start_time)"
exec 3<&-
exec 3>&-