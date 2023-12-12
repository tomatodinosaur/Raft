#########################################################
echo '***' Starting job count test.

rm -f mr-*

$TIMEOUT ../mrcoordinator ../pg*txt &
sleep 1

$TIMEOUT ../mrworker ../../mrapps/jobcount.so &
$TIMEOUT ../mrworker ../../mrapps/jobcount.so
$TIMEOUT ../mrworker ../../mrapps/jobcount.so &
$TIMEOUT ../mrworker ../../mrapps/jobcount.so

NT=`cat mr-out-0 | awk '{print $2}'`
if [ "$NT" -eq "8" ]
then
  echo '---' job count test: PASS
else
  echo '---' map jobs ran incorrect number of times "($NT != 8)"
  echo '---' job count test: FAIL
  failed_any=1
fi

wait
