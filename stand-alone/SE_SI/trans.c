/*
 * trans.c
 *
 *  Created on: Nov 10, 2015
 *      Author: xiaoxin
 */
/*
 * transaction actions are defined here.
 */
#include <malloc.h>
#include <sys/time.h>
#include <stdlib.h>
#include "trans.h"
#include "thread_global.h"
#include "data_record.h"
#include "lock_record.h"
#include "mem.h"
#include "proc.h"
#include "snapshot.h"
#include "data_am.h"
#include "transactions.h"
#include "config.h"
#include "thread_main.h"

IDMGR* CentIdMgr;
static TimeStampTz SetCurrentTransactionStartTimestamp(void);
static TimeStampTz SetCurrentTransactionStopTimestamp(void);

void InitTransactionIdAssign(void)
{
	Size size;
	size=sizeof(IDMGR);

	CentIdMgr=(IDMGR*)malloc(size);

	if(CentIdMgr==NULL)
	{
		printf("malloc error for IdMgr.\n");
		return;
	}

	CentIdMgr->curid=1;
	CentIdMgr->latestcompletedId=InvalidTransactionId;
}

/*
 * assign transaction ID for each transaction.
 */
TransactionId AssignTransactionId(void)
{
	TransactionId tid;
	pthread_mutex_lock(&CentIdMgr->IdLock);

	tid=CentIdMgr->curid++;

	pthread_mutex_unlock(&CentIdMgr->IdLock);

	if(tid <= CentIdMgr->maxid)
		return InvalidTransactionId;
	return tid;
}

void InitTransactionStructMemAlloc(void)
{
	TransactionData* td;
	THREAD* threadinfo;
	char* memstart;
	Size size;

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	memstart=threadinfo->memstart;

	size=sizeof(TransactionData);

	td=(TransactionData*)MemAlloc((void*)memstart,size);

	if(td==NULL)
	{
		printf("memalloc error.\n");
		return;
	}

	pthread_setspecific(TransactionDataKey,td);

	//to set snapshot-data memory.
	InitTransactionSnapshotDataMemAlloc();

	//to set data memory.
	InitDataMemAlloc();

	//to set data-lock memory.
	InitDataLockMemAlloc();
}

/*
 *start a transaction running environment, reset the
 *transaction's information for a new transaction.
 */
void StartTransaction(void)
{
	TransactionData* td;
	THREAD* threadinfo;
	char* memstart;
	int index;

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	memstart=threadinfo->memstart;


	td=(TransactionData*)pthread_getspecific(TransactionDataKey);

	//to set snapshot-data memory.
	InitTransactionSnapshotData();

	//to set data memory.
	InitDataMem();

	//to set data-lock memory.
	InitDataLockMem();

	//assign transaction ID here.
	td->tid=AssignTransactionId();

	if(!TransactionIdIsValid(td->tid))
	{
		printf("transaction ID assign error.\n");
		return;
	}
	td->starttime=SetCurrentTransactionStartTimestamp();
	td->stoptime=InvalidTimestamp;

	index=threadinfo->index;
	ProcArrayAdd(index);

	//get transaction snapshot data here.
	GetTransactionSnapshot();
}

void CommitTransaction(void)
{
	TimeStampTz ctime;
	TransactionId tid;
	TransactionData* tdata;
	THREAD* threadinfo;
	int index;

	tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
	tid=tdata->tid;

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index=threadinfo->index;

	ctime=SetCurrentTransactionStopTimestamp();

	CommitDataRecord(tid,ctime);

	AtEnd_ProcArray(index);

	DataLockRelease();

	TransactionMemClean();
}

void AbortTransaction(int trulynum)
{
	TransactionId tid;
	TransactionData* tdata;
	THREAD* threadinfo;
	int index;

	tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
	tid=tdata->tid;

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index=threadinfo->index;

	AbortDataRecord(tid, trulynum);

	//once abort, clean the process array after clean updated-data.
	AtEnd_ProcArray(index);

	DataLockRelease();

	TransactionMemClean();
}

void TransactionRunSchedule(void* args)
{
	//to run transactions according to args.
	int type;
	int rv;
	terminalArgs* param=(terminalArgs*)args;
	type=param->type;

	if(type==0)
	{
		printf("begin LoadData......\n");
		LoadData();
	}
	else
	{
		printf("ready to execute transactions...\n");

		rv=pthread_barrier_wait(&barrier);
		if(rv != 0 && rv != PTHREAD_BARRIER_SERIAL_THREAD)
		{
			printf("Couldn't wait on barrier\n");
			exit(-1);
		}


		printf("begin execute transactions...\n");
		executeTransactions(transactionsPerTerminal, param->whse_id, param->dist_id, param->StateInfo);
	}
}

/*
 * get current transaction data, return it's pointer.
 */
PROC* GetCurrentTransactionData(void)
{
	PROC* proc;
	proc=(PROC*)pthread_getspecific(TransactionDataKey);
	return proc;
}

TimeStampTz SetCurrentTransactionStartTimestamp(void)
{
	return GetCurrentTimestamp();
}

TimeStampTz SetCurrentTransactionStopTimestamp(void)
{
	return GetCurrentTimestamp();
}

/*
 * function: physically insert, update, and delete tuples.
 *@return:'1' for success, '-1' for rollback.
 *@input:'index':record the truly done data-record's number when PreCommit failed.
 */
int PreCommit(int* index)
{
	char* DataMemStart, *start;
	int num,i,result;
	DataRecord* ptr;
	TransactionData* tdata;

	tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);

	DataMemStart=(char*)pthread_getspecific(DataMemKey);
	start=DataMemStart+DataNumSize;

	num=*(int*)DataMemStart;

	//sort the data-operation records.
	DataRecordSort((DataRecord*)start, num);

	for(i=0;i<num;i++)
	{
		ptr=(DataRecord*)(start+i*sizeof(DataRecord));

		switch(ptr->type)
		{
		case DataInsert:
			result=TrulyDataInsert(ptr->data, ptr->table_id, ptr->index, ptr->tuple_id, ptr->value);
			break;
		case DataUpdate:
			result=TrulyDataUpdate(ptr->data, ptr->table_id, ptr->index, ptr->tuple_id, ptr->value);
			break;
		case DataDelete:
			result=TrulyDataDelete(ptr->data, ptr->table_id, ptr->index, ptr->tuple_id);
			break;
		default:
			printf("PreCommit:shouldn't arrive here.\n");
		}
		if(result == -1)
		{
			*index=i;
			return -1;
		}
	}
	return 1;
}
