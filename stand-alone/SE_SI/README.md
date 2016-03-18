The code is ready for traditional snapshot isolation in single machine (shared everything architecture).
To run the code, following conditions should be satisfied basically:
+ Linux operating system.
+ GCC compiler.

## benchmark configure parameters

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
```