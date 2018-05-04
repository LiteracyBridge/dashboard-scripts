#!/usr/bin/env bash

function main() {
    clean
    create
    test
    clean
}

function clean() {
    rm -f aa.kvp bb.kvp ab.csv ab2.csv ba.csv ba2.csv recip.kvp recip.csv recip.expected recip.map
}

function create() {
cat >aa.kvp << EndOfData
20180304T120900.100-10,op1,one:1,two:2,three:3,four:4,five:5,six:6,seven:7,optional1
20180304T120901.000-10,op2,two:2,seven:7,six:6,one:1,five:5,three:3,four:4,optional2
20180304T120902.000-10,op1,one:1,seven:7,two:2,three:3,five:5,six:6,four:4,optional3
20180304T120903.100-10,op2,two:2,three:3,four:4,seven:7,five:5,six:6,one:1,optional1
20180304T120904.100-10,op1,five:5,three:3,seven:7,six:6,one:1,two:2,four:4,optional2
EndOfData

cat >bb.kvp << EndOfData
20180304T121000.100-10,op1,eleven:11,two:2,three:3,four:4,five:5,six:6,seven:7,optional1
20180304T121001.000-10,op2,two:2,seven:7,six:6,eleven:11,five:5,three:3,four:4,optional2
20180304T121002.000-10,op1,eleven:11,seven:7,two:2,three:3,five:5,six:6,four:4,optional3
20180304T121003.100-10,op2,two:2,three:3,four:4,seven:7,five:5,six:6,eleven:11,optional1
20180304T121004.100-10,op1,five:5,three:3,seven:7,six:6,eleven:11,two:2,four:4,optional2
EndOfData

cat >recip.kvp << EndOfData
20180304T121000.100-10,op1,project:proj1,community:some directory name,value:lookup
20180304T121001.000-10,op2,project:proj2,community:other directory name,value:lookup
20180304T121002.000-10,op1,project:proj3,community:different directory name,value:missing
20180304T121003.100-10,op2,project:proj4,community:random directory name,recipientid:c5678,value:provided
EndOfData

cat >recip.expected << EndOfData
timestamp,project,community,value,recipientid
20180304T121000.100-10,proj1,some directory name,lookup,a1234
20180304T121001.000-10,proj2,other directory name,lookup,b3456
20180304T121002.000-10,proj3,different directory name,missing,
20180304T121003.100-10,proj4,random directory name,provided,c5678
EndOfData

cat >recip.map << EndOfData
recipientid,project,directory
a1234,proj1,"some directory name"
b3456,proj2,"other directory name"
EndOfData
}

function test() {
    set -x
    ./kv2csv.py aa.kvp bb.kvp --out ab.csv
    ./kv2csv.py aa.kvp bb.kvp --2pass --out ab2.csv
    ./kv2csv.py bb.kvp aa.kvp --out ba.csv
    ./kv2csv.py bb.kvp aa.kvp --2pass --out ba2.csv
    cmp ab.csv ab2.csv
    cmp ba.csv ba2.csv
    echo Next cmp should differ>/dev/null
    cmp ab.csv ba.csv
    ./kv2csv.py recip.kvp --map recip.map --out recip.csv --columns timestamp +
    cmp recip.expected recip.csv
    set +x
}

main "$@"