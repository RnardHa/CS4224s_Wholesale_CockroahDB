# CS4224s_Wholesale_CockroahDB
 
## 1. Introduction
Evaluate the performance of CockroachDB when large number of transactions are executed.

## 2. Installation Instructions
### Install CockroachDB in Linux
Installation for other OS can be found here https://www.cockroachlabs.com/docs/v20.1/install-cockroachdb-linux <br>
### Download the CockroachDB archive for Linux:
SSH into machine where you want the node to run then go to directory of where you want CockroachDB to be installed.
```
cd /temp/cs4224<xx> xx->group name
wget -qO- https://binaries.cockroachdb.com/cockroach-v20.1.8.linux-amd64.tgz | tar  xvz
```
### Export path, so it is easy to execute cockroach command from any shell:
```
export PATH=/temp/cs4224<xx>/cockroach-v20.1.6.linux-amd64:$PATH
```

## 3. Start 5 Nodes and a cluster
In each server, run the following cockroach command:
### Node 1
```
cockroach start --insecure --store=node1 --advertise-addr=<IP ADDRESS>:25267 --join=<IP ADDRESS>:25267,<IP ADDRESS>:25268,<IP ADDRESS>:25269 --http-addr=<IP ADDRESS>:8180 --listen-addr=<IP ADDRESS>:25267
```
### Node 2
```
cockroach start --insecure --store=node2 --advertise-addr=<IP ADDRESS>:25268 --join=<IP ADDRESS>:25267,<IP ADDRESS>:25268,<IP ADDRESS>:25269 --http-addr=<IP ADDRESS>:9181 --listen-addr=<IP ADDRESS>:25268
```
### Node 3
```
cockroach start --insecure --store=node3 --advertise-addr=<IP ADDRESS>:25269 --join=<IP ADDRESS>:25267,<IP ADDRESS>:25268,<IP ADDRESS>:25269 --http-addr=<IP ADDRESS>:8182 --listen-addr=<IP ADDRESS>:25269
```
### Node 4
```
cockroach start --insecure --store=nod4 --advertise-addr=<IP ADDRESS>:25270 --join=<IP ADDRESS>:25267,<IP ADDRESS>:25268,<IP ADDRESS>:25269 --http-addr=<IP ADDRESS>:8183 --listen-addr=<IP ADDRESS>:25270
```
### Node 5
```
cockroach start --insecure --store=nod5 --advertise-addr=<IP ADDRESS>:25271 --join=<IP ADDRESS>:25267,<IP ADDRESS><IP ADDRESS>:25268,<IP ADDRESS>:25269 --http-addr=<IP ADDRESS>:8184 --listen-addr=<IP ADDRESS>:25271
```
### Start Cluster
Run this in node 1
```
cockroach init --insecure --host=<IP ADDRESS>:25267
```

## 4. Check the if the cluster is working
Run the cockroach sql command against node 1
```
cockroach sql --insecure --host=<IP ADDRESS>:25267
```
OR
In your browser enter node 1
```
<IP ADDRESS>:<port>
Example: http://192.168.48.194:8180/
```

## 5. Setup load balancing
### Install HAProxy
```
apt-get install haproxy
```
### Run cockroach gen haproxy
```
cockroach gen haproxy --insecure \
--host=<address of any node> \
--port=25267
```
This will generate a file called haproxy.cfg, change the configuration as needed
### Export path, so it is easy to execute haproxy command from any shell:
```
export PATH=/<FILE LOCATION>/haproxy-2.2.0:$PATH
```
### Configure HAProxy
Change bind value to 25273
```
listen.psql
    bind :25273
    ...
    ...
```
### Start HAProxy
```
haproxy -f haproxy.cfg
```

## 6. Load data into database
### Save file to be loaded in node 1
1. Create a file 'extern'
2. Put all the files to be loaded inside 'extern'
### Load data
```
bash LoadData.sh
```

## 7. Run application
### In the folder containing source codes
```
python Driver.py -EN <Experiment Number> -> <5/6/7/8>
```
Note: Experiment 5 & 7 -> 4 servers active, 6 & 8 -> 5 servers active <br>
      Recommended to run 6 & 8 followed by 5 & 7
      
### Run with 4 servers
In order to simulate only 4 servers are running, we kill one of the nodes, E.g. node 5
```
cockroach quit --insecure --host=<IP ADDRESS>:25271
```
### Get DBState.csv file
```
python DBState.py -EN <Experiment Number>
```
### Get Throughput.csv file
```
python Throughput.py
```
