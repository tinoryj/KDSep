#!/bin/bash

#scripts/runUpdateTest_jhli.sh req10M op10M fc1 fl4000
#scripts/runUpdateTest_jhli.sh kv req10M op10M fc1 fl4000
#scripts/runUpdateTest_jhli.sh kd req10M op10M fc1 fl4000
#scripts/runUpdateTest_jhli.sh kvkd req10M op10M fc1 fl4000
#scripts/runUpdateTest_jhli.sh req5M op1M fc2 fl4000
#scripts/runUpdateTest_jhli.sh kv req5M op1M fc2 fl4000
#scripts/runUpdateTest_jhli.sh kd req5M op1M fc2 fl4000
#scripts/runUpdateTest_jhli.sh kvkd req5M op1M fc2 fl4000
#scripts/runUpdateTest_jhli.sh req5M op2M fc2 fl4000
#scripts/runUpdateTest_jhli.sh kv req5M op2M fc2 fl4000
#scripts/runUpdateTest_jhli.sh kd req5M op2M fc2 fl4000
#scripts/runUpdateTest_jhli.sh kvkd req5M op2M fc2 fl4000

#scripts/runUpdateTest_jhli.sh req10M op2M fc8 fl500
#scripts/runUpdateTest_jhli.sh kv req10M op2M fc8 fl500
#scripts/runUpdateTest_jhli.sh kd req1M op2M fc8 fl500 load
#scripts/runUpdateTest_jhli.sh kd req100k op2M fc8 fl500 load
#scripts/runUpdateTest_jhli.sh req1M op2M fc8 fl500 load

req=("10M" "5M" "2500K" "1250K" "10M" "5M" "2500K" "1250K")
ops=("10M" "5M" "2500K" "1250K" "10M" "5M" "2500K" "1250K")
fcs=("8" "8" "8" "8" "4" "4" "4" "4")
fls=("500" "1000" "2000" "4000" "1000" "2000" "4000" "8000")

req=("10M" "5M" "2500K" "1250K" "10M" "5M" "2500K" "1250K")
ops=("2M" "2M" "2M" "1M" "2M" "2M" "2M" "1M")
fcs=("8" "8" "8" "8" "4" "4" "4" "4")
fls=("500" "1000" "2000" "4000" "1000" "2000" "4000" "8000")

wbrs=("5" "10" "15" "20" "25" "30" "1000")
wbrs=("1000")
scripts/runUpdateTest_jhli.sh kvkd req10M op2M fc4 fl1000 load 
exit
for ((i=0; i<${#wbrs[@]}; i++)); do
    scripts/runUpdateTest_jhli.sh kvkd req10M op2M fc4 fl1000 ow0.01 wbr5 # ${wbrs[$i]}
done
exit

for ((i=4; i<8; i++)); do
    scripts/runUpdateTest_jhli.sh req${req[$i]} op${ops[$i]} fc${fcs[$i]} fl${fls[$i]} 
    scripts/runUpdateTest_jhli.sh kv req${req[$i]} op${ops[$i]} fc${fcs[$i]} fl${fls[$i]} 
    scripts/runUpdateTest_jhli.sh kd req${req[$i]} op${ops[$i]} fc${fcs[$i]} fl${fls[$i]} gc
    scripts/runUpdateTest_jhli.sh kvkd req${req[$i]} op${ops[$i]} fc${fcs[$i]} fl${fls[$i]} gc
done
exit

#scripts/runUpdateTest_jhli.sh kd req10M op2M fc4 fl1000

#scripts/runUpdateTest_jhli.sh bkv req10M op2M fc4 fl1000 

bs=("512" "256" "128" "64")

for ((i=0; i<${#bs[@]}; i++)); do
    scripts/runUpdateTest_jhli.sh kvkd req10M op10M fc4 fl1000 bs$(( ${bs[$i]} * 1024 ))
    exit
done
exit

scripts/runUpdateTest_jhli.sh kvkd req10M op2M fc4 fl1000
scripts/runUpdateTest_jhli.sh kvkd req10M op2M fc4 fl1000

#scripts/runUpdateTest_jhli.sh kvkd req10M op2M fc4 fl1000 ow0.01
#scripts/runUpdateTest_jhli.sh kd req10M op2M fc4 fl1000 ow0.01 
#scripts/runUpdateTest_jhli.sh kv req10M op2M fc4 fl1000 ow0.01
#scripts/runUpdateTest_jhli.sh req10M op2M fc4 fl1000 ow0.01

#scripts/runUpdateTest_jhli.sh kd req5M op2M fc8 fl500 load
#scripts/runUpdateTest_jhli.sh kvkd req10M op2M fc8 fl500 load
#scripts/runUpdateTest_jhli.sh kvkd req10M op2M fc8 fl500 
