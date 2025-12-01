#!/bin/bash
brz_home="/data1/ci/breeze"
mkdir -p $brz_home

docker ps -a | grep breeze_ci_mysql4623 && docker rm -f breeze_ci_mysql4623
docker ps -a | grep breeze_ci_mysql4624 && docker rm -f breeze_ci_mysql4624
docker run --rm --name breeze_ci_mysql4623  -p 4623:3306 -d parabala/mysqlci_with_schema:v0.0.2
docker run --rm --name breeze_ci_mysql4624  -p 4624:3306 -d parabala/mysqlci_with_schema:v0.0.2

container_name=breeze_github_ci
docker ps -a | grep "$container_name" && docker rm -f "$container_name"


docker run --rm -d \
  -v "$brz_home":/data1/resource/breeze \
  -p 8080:8080 \
  -p 13739:13739 \
  -p 13740:13740 \
  -p 13741:13741 \
  -p 13742:13742 \
  -p 56378:56378 \
  -p 56379:56379 \
  -p 56380:56380 \
  -p 56381:56381 \
  -p 56382:56382 \
  -p 56383:56383 \
  -p 56384:56384 \
  -p 56385:56385 \
  -p 56386:56386 \
  -p 56387:56387 \
  -p 56388:56388 \
  -p 56389:56389 \
  -p 56390:56390 \
  -p 56391:56391 \
  -p 56392:56392 \
  -p 56393:56393 \
  -p 8010:8010 \
  -p 8011:8011 \
  -p 8012:8012 \
  -p 8013:8013 \
  -p 8775:8775 \
  -p 8776:8776 \
  -p 8777:8777 \
  -p 8778:8778 \
  --name "$container_name" \
  viciousstar/breeze:githubci120

# rm -rf $brz_home/*
mkdir -p $brz_home/logs
mkdir -p $brz_home/snapshot
mkdir -p $brz_home/socks
touch $brz_home/socks/127.0.0.1:8080+config+cloud+redis+testbreeze+redismeshtest@redis:56810@rs
touch $brz_home/socks/127.0.0.1:8080+config+cloud+redis+testbreeze+redismeshtestm@redis:56812@rs
touch $brz_home/socks/127.0.0.1:8080+config+v1+cache.service.testbreeze.pool.yf+all:meshtest@mc:9301@cs
touch $brz_home/socks/127.0.0.1:8080+config+cloud+counterservice+testbreeze+meshtest@redis:9302@rs
touch $brz_home/socks/127.0.0.1:8080+config+cloud+phantom+testbreeze+phantomtest@phantom:9303@pt
touch $brz_home/socks/127.0.0.1:8080+config+cloud+kv+testbreeze+kvmeshtest@kv:3306@kv
touch $brz_home/socks/127.0.0.1:8080+config+cloud+vector+testbreeze+vectortest@vector:3308@vector
touch $brz_home/socks/127.0.0.1:8080+config+cloud+mq+testbreeze+mcqmeshtest_1@msgque:56815@mq

cargo build
nohup ./target/debug/agent --discovery vintage://127.0.0.1:8080 --snapshot $brz_home/snapshot --service-path $brz_home/socks --log-dir $brz_home/logs --port 9984 --metrics-probe 8.8.8.8:53 --log-level info --idc-path 127.0.0.1:8080/3/config/breeze/idc_region --key-path .github/workflows/private_key.pem > $brz_home/logs/log.file  2>&1 &

pid=$!

export redis=localhost:56810
export redis_with_slave=localhost:56812
export counterservice=localhost:9302
export mc=localhost:9301
export phantom=localhost:9303
export mysql=localhost:3306
export vector=localhost:3308
export mq=localhost:56815
export min_key=1
export max_key=10000
export socks_dir=$brz_home/socks

RUST_BACKTRACE=1 cargo test -p tests

#等待mesh初始化，最多等待两分钟
port_list=(56810 56812 9302 9301 9303 3306 3308)  # 端口列表
start=$(date +%s)  # 获取当前时间戳

while true; do
    now=$(date +%s)  # 获取当前时间戳
    diff=$((now - start))  # 计算时间差
    if [ $diff -lt 120 ]; then  # 如果时间差小于120秒
        all_listened=true
        for port in "${port_list[@]}"; do
            if ! netstat -an | grep -q ":$port.*LISTEN"; then
                echo "Port $port is not being listened to. Sleeping for 5 seconds..."
                all_listened=false
                sleep 5  # 等待5秒
                break
            fi
        done
        if $all_listened; then
            echo "All ports are being listened to. Exiting loop."
            break  # 退出循环
        fi
    else
        echo "Two minutes have passed. Exiting loop."
        break  # 退出循环
    fi
done

RUST_BACKTRACE=1 cargo test -p tests_integration --features github_workflow

kill -9 $pid

