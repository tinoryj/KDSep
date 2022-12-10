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
ops=("5M" "2500K" "1250K" "625K" "5M" "2500K" "1250K" "625K")
fcs=("8" "8" "8" "8" "4" "4" "4" "4")
fls=("500" "1000" "2000" "4000" "1000" "2000" "4000" "8000")

scripts/runUpdateTest_jhli.sh testkd req${req[$i]} op${ops[$i]} fc${fcs[$i]} fl${fls[$i]} gc minbits15
exit

for ((i=0; i<8; i++)); do
  if [[ $i -ne 0 ]]; then
    scripts/runUpdateTest_jhli.sh req${req[$i]} op${ops[$i]} fc${fcs[$i]} fl${fls[$i]}
    scripts/runUpdateTest_jhli.sh kd req${req[$i]} op${ops[$i]} fc${fcs[$i]} fl${fls[$i]}
  fi
  scripts/runUpdateTest_jhli.sh kv req${req[$i]} op${ops[$i]} fc${fcs[$i]} fl${fls[$i]}
  scripts/runUpdateTest_jhli.sh kvkd req${req[$i]} op${ops[$i]} fc${fcs[$i]} fl${fls[$i]}
done

#scripts/runUpdateTest_jhli.sh kd req5M op2M fc8 fl500 load
#scripts/runUpdateTest_jhli.sh kvkd req10M op2M fc8 fl500 load
#scripts/runUpdateTest_jhli.sh kvkd req10M op2M fc8 fl500 
