#########################################################
echo '***' Starting reduce parallelism test.

rm -f mr-*

$TIMEOUT ../mrcoordinator ../pg*txt &
sleep 1

$TIMEOUT ../mrworker ../../mrapps/rtiming.so &
$TIMEOUT ../mrworker ../../mrapps/rtiming.so

NT=`cat mr-out* | grep '^[a-z] 2' | wc -l | sed 's/ //g'`
if [ "$NT" -lt "2" ]
then
  echo '---' too few parallel reduces.
  echo '---' reduce parallelism test: FAIL
  failed_any=1
else
  echo '---' reduce parallelism test: PASS
fi

wait

#########################################################