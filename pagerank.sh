ALGORITHM=Pagerank
WORKERS=5
GRAPH=input/pagerank
RESULT=result/pagerank
NODES=2000000
SNAPSHOT=1
TERMTHRESH=0.0001
BUFMSG=64000
BUFMSG2=50000
DEGREE=10
SHARD=4
PORTION=0.2



./maiter  --runner=$ALGORITHM --workers=$WORKERS --graph_dir=$GRAPH --result_dir=$RESULT --num_nodes=$NODES --snapshot_interval=$SNAPSHOT --portion=$PORTION --termcheck_threshold=$TERMTHRESH --bufmsg=$BUFMSG --bufmsg2=$BUFMSG2 --degree=$DEGREE --shard=$SHARD --v=3 > log

