/*
 * threadmain.h
 *
 *  Created on: Nov 11, 2015
 *      Author: xiaoxin
 */

#ifndef THREADMAIN_H_
#define THREADMAIN_H_

#include <stdint.h>
#include <pthread.h>

#include "transactions.h"

extern void ThreadRun(int num);

extern void InitSys(void);

extern void ExitSys(void);

extern void RunTerminals(int numTerminals);

extern void runTerminal(int terminalWarehouseID, int terminalDistrictID, pthread_t *tid, pthread_barrier_t *barrier, TransState* StateInfo);

extern void dataLoading(void);

#endif /* THREADMAIN_H_ */
