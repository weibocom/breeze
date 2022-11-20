#!/bin/bash
# phantom
remote_ip=${1-127.0.0.1}
action=${2-recover}


if [ "$action" = "recover" ]; then
    curl -XPUT "http://$remote_ip:8080/config/cloud/phantom/testbreeze/phantomtest" -d '{"data":"basic:
  hash: crc32-point
  listen: 8775,8776,8777,8778
  resource_type: phantom
  timeout: 60000
backends:
  -
   name: ha1
   access_mod: rw
   distribution: range-1024
   servers:
     - '"$remote_ip"':8775
     - '"$remote_ip"':8776
     - '"$remote_ip"':8777
     - '"$remote_ip"':8778

  -
   name: ha2
   access_mod: rw
   distribution: range-1024
   servers:
     - '"$remote_ip"':8775
     - '"$remote_ip"':8776
     - '"$remote_ip"':8777
     - '"$remote_ip"':8778
"}'
fi

if [ "$action" = "backend_changed" ]; then
    curl -XPUT "http://$remote_ip:8080/config/cloud/phantom/testbreeze/phantomtest" -d '{"data":"basic:
  hash: crc32-point
  listen: 8776
  resource_type: phantom
  timeout: 60000
backends:
  -
   name: ha1
   access_mod: rw
   distribution: range-1024
   servers:
     - '"$remote_ip"':8776
     - '"$remote_ip"':8776
     - '"$remote_ip"':8776
     - '"$remote_ip"':8776

  -
   name: ha2
   access_mod: rw
   distribution: range-1024
   servers:
     - '"$remote_ip"':8776
     - '"$remote_ip"':8776
     - '"$remote_ip"':8776
     - '"$remote_ip"':8776
"}'
fi