The code is ready for post snapshot isolation in single machine (shared everything architecture).
To run the code, following conditions should be satisfied basically:
+ Linux operating system.
+ GCC compiler.
+ A cluster of machines interconnected through TCP/IP sockets or a single machine support TCP/IP protocol.

## config.txt

In the MPP platform with shared nothing architecture, we choose a machine node to act as the master node, and others as the slave nodes, while the master node here is no more a central coordinator, it is just in use at the starting step of the distributed system. To run the code, we should deploy the source code under the directory `SI_master` in the master node, and the source code under the directory `SI_slave` in the slave nodes. Following are the configure details for the master node and slave nodes.

### master

```
masterip: 127.0.0.1
messageport: 8000
paramport: 8001
nodenum: 1
threadnum: 1
clientport: 4000
```

+ masterip: If you run the distributed system in a single machine, just use the loop address `127.0.0.1`, otherwise replace it with you master node ip.
+ messageport && paramport: These two port are used the objective:  `synchronize the master and slave` , the slave nodes configure file should also have two ports same with master, this is very important! 
+ nodenum: The node number of slave nodes in the distributed system.
+ threadnum: The parallel number of threads in one slave node.
+ clientport: the port is send as a parameter from the master node to the slave nodes.

### slave

```
masterip: 127.0.0.1
messageport: 8000
paramport: 8001
nodeid: 0
nodeip0: 127.0.0.1
nodeip1: 127.0.0.1
nodeip2: 127.0.0.1
nodeip3: 127.0.0.1
nodeip4: 127.0.0.1

......

```

+ masterip: The same as above.
+ messageport && paramport: Should be coincident with master node.
+ nodeid: The unique ID for every slave node in the system, this ID should be consecutive integer start with 0.
+ nodeip+nodeid: IP list of all the slave nodes in the distributed system.

## benchmark configure parameters (in slave source code directory)

To run the code in different conditions, we should change the value of some parameters. Those parameters are all in the source file 'config.c', following are the details of those parameters:

```
int configWhseCount; /* number of warehouses, set to different value to test different data scale. */

int configCommitCount; /* max number of tuples operated in one transaction, or the max length of one transaction. */

int transactionsPerTerminal; /* number of transactions per terminal. */

int paymentWeightValue, orderStatusWeightValue, deliveryWeightValue, stockLevelWeightValue; /* ratio of each transaction in one terminal */

int NumTerminals; /* the num of terminals when running TPCC benchmark, make sure that 'NumTerminals' <= 'MAXPROCS'. */

int OrderMaxNum; /* the limited max number of new orders for each district, if there is no enough space, this value should be larger. */

int MaxDataLockNum; /* the max number of wr-locks held in one transaction, there is no need to change this value. */
```

there is no need to change other parameters except above listed parameters.

To compile the code, we should add the following options to the gcc command:

```
"-lpthread" /* to support the running of multi-threads. */