# Yahoo! Cloud System Benchmark
# Workload A: Update heavy workload
#   Application example: Session store recording recent actions
#                        
#   Read/update ratio: 50/50
#   Default data size: 1 KB records (10 fields, 100 bytes each, plus key)
#   Request distribution: zipfian

recordcount=50000
operationcount=50000
workload=com.yahoo.ycsb.workloads.CoreWorkload

readallfields=true

readproportion=0.1
updateproportion=0
readmodifywriteproportion=0.8
overwriteproportion=0.1
scanproportion=0
insertproportion=0


requestdistribution=zipfian
