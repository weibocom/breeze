#!/bin/sh
brz_home="/data1/ci/breeze"
mkdir -p $brz_home

container_name=breeze_github_ci
docker ps -a | grep "$container_name" && docker rm -f "$container_name"

docker run --rm -d -v $brz_home:/data1/resource/breeze  --net="host"  --name "$container_name" xinxin111/breeze:githubci106

# rm -rf $brz_home/*
mkdir -p $brz_home/logs
mkdir -p $brz_home/snapshot
mkdir -p $brz_home/socks
touch $brz_home/socks/127.0.0.1:8080+config+cloud+redis+testbreeze+redismeshtest@redis:56810@rs
touch $brz_home/socks/127.0.0.1:8080+config+cloud+redis+testbreeze+redismeshtestm@redis:56812@rs
touch $brz_home/socks/127.0.0.1:8080+config+v1+cache.service.testbreeze.pool.yf+all:meshtest@mc:9301@cs
touch $brz_home/socks/127.0.0.1:8080+config+cloud+counterservice+testbreeze+meshtest@redis:9302@rs
touch $brz_home/socks/127.0.0.1:8080+config+cloud+phantom+testbreeze+phantomtest@phantom:9303@pt

cargo build
nohup ./target/debug/agent --discovery vintage://127.0.0.1:8080 --snapshot $brz_home/snapshot --service-path $brz_home/socks --log-dir $brz_home/logs --port 9984 --metrics-probe 8.8.8.8:53 --log-level info --idc-path 127.0.0.1:8080/3/config/breeze/idc_region > $brz_home/logs/log.file  2>&1 &

pid=$!

export redis=localhost:56810
export redis_with_slave=localhost:56812
export counterservice=localhost:9302
export mc=localhost:9301
export phantom=localhost:9303
export min_key=1
export max_key=10000
export socks_dir=$brz_home/socks

cargo test -p tests

sleep 10

cargo test -p tests_integration --features github_workflow

kill -9 $pid
