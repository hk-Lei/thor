#!/bin/sh

echo ""
ps -ef | grep -v grep | grep ConnectStandalone | awk '{print $2 "\t" $1 "\t" $23}'
echo ""

pids=$(ps -ef | grep -v grep | grep ConnectStandalone | awk '{print $2}')
pids=$(echo $pids | sed -e "s/\s/|/g")

echo -n "请输入要停止的 pid (上述列表中最前面的数字) 或者 all (停止全部) : "

read PID

if [[ "$PID" =~ ^($pids)$ ]]; then
  echo -e "\nStopping $PID \n"
  #kill $PID
elif [ "$PID" == "all" ]; then
  echo "ps -ef | grep -v grep | grep ConnectStandalone | awk '{print $2}' | xrags kill"
else
  echo -e "\n$PID is not in the list: $pids \n"
  exit 0
fi