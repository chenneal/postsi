/*
 * config.h
 *
 *  Created on: Jan 5, 2016
 *      Author: xiaoxin
 */

#ifndef CONFIG_H_
#define CONFIG_H_

#include <stdlib.h>
#include <stdint.h>
/*
 * parameters configurations here.
 */

//max memory size for recording tuple accessed in one transaction.
#define DataMenMaxSize 128*1024

//the limited max number of terminals when running TPCC or other benchmark.
#define MAXPROCS 64

//the number of tables in TPCC
#define TABLENUM  9

//the max number of versions attached to each tuple.
#define VERSIONMAX 20

extern int configWhseCount;

extern  int configDistPerWhse;

extern int configCustPerDist;

extern int MaxBucketSize;

extern int configUniqueItems;

extern int configCommitCount;

extern int transactionsPerTerminal;
extern int paymentWeightValue;
extern int orderStatusWeightValue;
extern int deliveryWeightValue;
extern int stockLevelWeightValue;
extern int limPerMin_Terminal;

extern int NumTerminals;

extern int OrderMaxNum;

extern int MaxDataLockNum;

extern void InitConfig(void);

#endif /* CONFIG_H_ */
