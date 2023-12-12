rm -f mr-*
find ../intermediate/ -type f -delete
echo '***' Starting early exit test.

DF=anydone$$
rm -f $DF

($TIMEOUT ../mrcoordinator ../pg*txt ; touch $DF) &

# give the coordinator time to create the sockets.
sleep 1

# start multiple workers.
($TIMEOUT ../mrworker ../../mrapps/early_exit.so ; touch $DF) &
($TIMEOUT ../mrworker ../../mrapps/early_exit.so ; touch $DF) &
($TIMEOUT ../mrworker ../../mrapps/early_exit.so ; touch $DF) &

# wait for any of the coord or workers to exit.
# `jobs` ensures that any completed old processes from other tests
# are not waited upon.
jobs &> /dev/null
if [[ "$OSTYPE" = "darwin"* ]]
then
  # bash on the Mac doesn't have wait -n
  while [ ! -e $DF ]
  do
    sleep 0.2
  done
else
  # the -n causes wait to wait for just one child process,
  # rather than waiting for all to finish.
  wait -n
fi

rm -f $DF

# a process has exited. this means that the output should be finalized
# otherwise, either a worker or the coordinator exited early
sort mr-out* | grep . > mr-wc-all-initial

# wait for remaining workers and coordinator to exit.
wait

# compare initial and final outputs
sort mr-out* | grep . > mr-wc-all-final
if cmp mr-wc-all-final mr-wc-all-initial
then
  echo '---' early exit test: PASS
else
  echo '---' output changed after first worker exited
  echo '---' early exit test: FAIL
  failed_any=1
fi