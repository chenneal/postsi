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

extern void InitConfig(void);

#endif /* CONFIG_H_ */
