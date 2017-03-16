ALGORITHM=Simrank
WORKERS=2
GRAPH=input/simrank
RESULT=result/simrank
NODES=200
SNAPSHOT=5
TERMTHRESH=6.14
BUFMSG=500
PORTION=1



./maiter  --runner=$ALGORITHM --workers=$WORKERS --graph_dir=$GRAPH --result_dir=$RESULT --num_nodes=$NODES --snapshot_interval=$SNAPSHOT --portion=$PORTION --termcheck_threshold=$TERMTHRESH --bufmsg=$BUFMSG --v=0 > log


