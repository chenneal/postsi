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

struct TransIdMgr
{
	TransactionId curid;
	TransactionId maxid;
	TransactionId latestcompletedId;
	pthread_mutex_t IdLock;
};


typedef struct TransIdMgr IDMGR;

struct TransactionData
{
	TransactionId tid;

	TimeStampTz starttime;

	TimeStampTz stoptime;
};

typedef struct TransactionData TransactionData;

#define TransactionIdIsValid(tid) (tid != InvalidTransactionId)

extern IDMGR* CentIdMgr;

extern void InitTransactionStructMemAlloc(void);

extern void InitTransactionIdAssign(void);

extern TransactionId AssignTransactionId(void);

extern void TransactionRunSchedule(void *args);

extern PROC* GetCurrentTransactionData(void);

extern void TransactionContextCommit(TransactionId tid, TimeStampTz ctime, int index);

extern void TransactionContextAbort(TransactionId tid, int index);

extern void StartTransaction(void);

extern void CommitTransaction(void);

extern void AbortTransaction(int trulynum);

extern int PreCommit(int* index);

#endif /* TRANS_H_ */
