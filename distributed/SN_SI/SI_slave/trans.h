/*
 * trans.h
 *
 *  Created on: Nov 10, 2015
 *      Author: xiaoxin
 */

#ifndef TRANS_H_
#define TRANS_H_

#include "type.h"
#include "proc.h"
#include "timestamp.h"

#define InvalidTransactionId ((TransactionId)0)

struct TransactionData
{
	TransactionId tid;

	TimeStampTz starttime;

	TimeStampTz stoptime;
};

typedef struct TransactionData TransactionData;

#define TransactionIdIsValid(tid) (tid != InvalidTransactionId)

extern void InitTransactionStructMemAlloc(void);

extern void TransactionLoadData(int i);

extern void TransactionRunSchedule(void* args);

extern void TransactionContextCommit(TransactionId tid, TimeStampTz ctime, int index);

extern void TransactionContextAbort(TransactionId tid, int index);

extern void StartTransaction(void);

extern void CommitTransaction(void);

extern void AbortTransaction(int trulynum);

extern int PreCommit(int* index);

extern int GetNodeId(int index);

extern int GetLocalIndex(int index);
#endif /* TRANS_H_ */
