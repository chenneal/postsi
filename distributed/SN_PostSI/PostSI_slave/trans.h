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

#define InvalidTransactionId ((TransactionId)0)

struct TransIdMgr
{
	TransactionId curid;
	TransactionId maxid;
	pthread_mutex_t IdLock;
};


typedef struct TransIdMgr IDMGR;

struct TransactionData
{
	TransactionId tid;

	StartId	sid_min;
	StartId sid_max;

	CommitId cid_min;
};

extern TransactionId thread_0_tid;

typedef struct TransactionData TransactionData;

#define TransactionIdIsValid(tid) (tid != InvalidTransactionId)

extern void InitTransactionIdAssign(void);

extern void ProcTransactionIdAssign(THREAD* thread);

extern TransactionId AssignTransactionId(void);

extern void StartTransaction(void);

extern int CommitTransaction(void);

extern void AbortTransaction(int trulynum);

extern void InitTransactionStructMemAlloc(void);



extern void TransactionRunSchedule(void *args);

extern PROC* GetCurrentTransactionData(void);

extern void TransactionContextCommit(TransactionId tid, CommitId cid);

extern void TransactionContextAbort(TransactionId tid);

extern int PreCommit(int* index);

extern int GetNodeId(int index);

extern int GetLocalIndex(int index);

#endif /* TRANS_H_ */
