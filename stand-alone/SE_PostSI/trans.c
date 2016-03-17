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
#include <stdlib.h>
#include <assert.h>

#include "config.h"
#include "trans.h"
#include "thread_global.h"
#include "trans_conflict.h"
#include "data_record.h"
#include "lock_record.h"
#include "mem.h"
#include "proc.h"
#include "lock.h"
#include "data_am.h"
#include "transactions.h"
#include "translist.h"

static IDMGR* CentIdMgr;

StartId AssignTransactionStartId(TransactionId tid);

CommitId AssignTransactionCommitId(TransactionId tid);

int ConfirmIdAssign(StartId* sid, CommitId* cid);

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
}

/*
 * function: preassign a range of transaction-id for a thread (terminal).
 */
void ProcTransactionIdAssign(THREAD* thread)
{
	int index;
	index=thread->index;

	thread->curid=index*MaxTransId+1;
	thread->maxid=(index+1)*MaxTransId;
}

/*
 * function: assign a ID for a transaction.
 */
TransactionId AssignTransactionId(void)
{
	TransactionId tid;

	THREAD* threadinfo;

	threadinfo=pthread_getspecific(ThreadInfoKey);

	if(threadinfo->curid<=threadinfo->maxid)
		tid=threadinfo->curid++;
	else
	{
		printf("index=%d, curid=%d, maxid=%d\n", threadinfo->index, threadinfo->curid, threadinfo->maxid);
		return 0;
	}
	return tid;
}

/*
 * allocate memory space for variables needed during transaction.
 */
void InitTransactionStructMemAlloc(void)
{
	TransactionData* td;
	THREAD* threadinfo;
	PROC* proc;
	char* memstart;
	Size size;
	int index;

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

	//to set data memory.
	InitDataMemAlloc();

	//to set data-lock memory.
	InitDataLockMemAlloc();

	//to set read-list memory.
	InitReadListMemAlloc();
}

/*
 *start a transaction running environment, reset the
 *transaction's information for a new transaction.
 */
void StartTransaction(void)
{
	TransactionData* td;
	THREAD* threadinfo;
	PROC* proc;
	char* memstart;
	Size size;
	int index;

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	memstart=threadinfo->memstart;
	index=threadinfo->index;

	td=(TransactionData*)pthread_getspecific(TransactionDataKey);

	//to set data memory.
	InitDataMem();

	//to set data-lock memory.
	InitDataLockMem();

	//to set read-list memory.
	InitReadListMem();

	//assign transaction ID here.
	td->tid=AssignTransactionId();

	if(!TransactionIdIsValid(td->tid))
	{
		printf("transaction ID assign error.\n");
		return;
	}

	//transaction ID assignment succeeds.
	td->sid_min=0;
	td->cid_min=0;
	td->sid_max=MAXINTVALUE;

	proc=(PROC*)(procbase+index);

	//to hold lock here.
	//initialize those fields of process.
	//add lock to access.
	pthread_spin_lock(&ProcArrayElemLock[index]);

	proc->pid=pthread_self();
	proc->tid=td->tid;
	proc->sid_min=0;
	proc->sid_max=MAXINTVALUE;
	proc->cid_min=0;
	pthread_spin_unlock(&ProcArrayElemLock[index]);
}

int CommitTransaction(void)
{
	StartId sid;
	CommitId cid;
	TransactionId tid;
	TransactionData* tdata;
	THREAD* threadinfo;
	int index;
	int result;
	bool confirm=true;

	tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
	tid=tdata->tid;

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index=threadinfo->index;

	//assign StartId.
	sid=AssignTransactionStartId(tid);

	//assign CommitId.
	cid=AssignTransactionCommitId(tid);

	//confirm the assignment of transaction's time interval.
	if(!ConfirmIdAssign(&sid, &cid))
	{
		confirm=false;
	}

	//update other conflict transactions' time interval.
	if(confirm && !CommitInvisibleUpdate(index,sid,cid))
	{
		//transaction can commit.
		//to make sure that cid > sid.
		if(cid==sid)cid+=1;

		CommitDataRecord(tid,cid);

		result=0;
	}
	else
	{
		//set transaction in process abort to avoid unnecessary work.
		SetProcAbort(index);

		AbortDataRecord(tid, -1);

		result=-2;
	}

	DataLockRelease();

	//reset the row by 'index' of invisible-table/
	AtEnd_InvisibleTable(index);

	//by here, we consider that the transaction committed successfully or abort successfully.
	AtEnd_ProcArray(index);

	//clean the transaaction's memory.
	TransactionMemClean();

	return result;
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

	//set transaction in process abort to avoid unnecessary work.
	SetProcAbort(index);

	AbortDataRecord(tid,trulynum);

	DataLockRelease();

	//reset the row by 'index' of invisible-table/
	AtEnd_InvisibleTable(index);

	//by here, we consider that the transaction abort successfully.
	AtEnd_ProcArray(index);

	//clean the transaaction's memory.
	TransactionMemClean();
}

void TransactionRunSchedule(void* args)
{
	//to run transactions according to args.
	int i,result;
	int index, type;
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
		rv=pthread_barrier_wait(param->barrier);
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

/*
 * function: assign the transaction's StartId.
 */
StartId AssignTransactionStartId(TransactionId tid)
{
	StartId sid;
	THREAD* threadinfo;
	int index;

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);

	index=threadinfo->index;

	sid=GetTransactionSidMin(index);

	return sid;
}

/*
 * function: assign the tranaction's CommitId.
 */
CommitId AssignTransactionCommitId(TransactionId tid)
{
	CommitId cid;
	CommitId cid_min;
	THREAD* threadinfo;
	int index;

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index=threadinfo->index;
	//to hold lock here.

	cid_min=GetTransactionCidMin(index);

    cid=GetTransactionCid(index,cid_min);

	return cid;
}

/*
 * function: confirm the assignment of transaction's time interval.
 */
int ConfirmIdAssign(StartId* sid, CommitId* cid)
{
	THREAD* threadinfo;
	int index;
	PROC* proc;

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index=threadinfo->index;

	proc=procbase+index;

	pthread_spin_lock(&ProcArrayElemLock[index]);
	if(*sid > proc->sid_max)
	{
		//abort current transaction.
		pthread_spin_unlock(&ProcArrayElemLock[index]);
		return 0;
	}

	*cid=(proc->cid_min > (*cid))?proc->cid_min:(*cid);
	assert(*cid >= *sid);

	*cid=((*sid+1) > *cid) ? (*sid+1) : *cid;

	proc->cid=*cid;
	proc->complete=1;

	pthread_spin_unlock(&ProcArrayElemLock[index]);

	return 1;
}

void TransactionContextCommit(TransactionId tid, CommitId cid)
{
	CommitDataRecord(tid,cid);
	DataLockRelease();
	TransactionMemClean();
}

void TransactionContextAbort(TransactionId tid)
{
	DataLockRelease();
	TransactionMemClean();
}

/*
 * function: physically insert, update, and delete tuples.
 *@return:'1' for success, '-1' for rollback.
 *@input:'index':record the truly done data-record's number when PreCommit failed.
 */
int PreCommit(int* index)
{
	//return 1;
	char* DataMemStart, *start;
	int num,i,result;
	DataRecord* ptr;
	TransactionData* tdata;
	THREAD* threadinfo;

	TransactionId tid;
	int proc_index;

	tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);

	tid=tdata->tid;
	proc_index=threadinfo->index;

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
			if(ptr->tuple_id <= 0)
			{
				printf("error.\n");
				exit(-1);
			}
			result=TrulyDataInsert(ptr->table_id, ptr->index, ptr->tuple_id, ptr->value);
			break;
		case DataUpdate:
			result=TrulyDataUpdate(ptr->table_id, ptr->index, ptr->tuple_id, ptr->value);
			break;
		case DataDelete:
			result=TrulyDataDelete(ptr->table_id, ptr->index, ptr->tuple_id);
			break;
		default:
			printf("PreCommit:shouldn't arrive here.\n");
		}
		if(result == -1)
		{
			//return to rollback.
			*index=i;
			return -1;
		}
	}

	//record the read-write anti-dependency between current transaction and active transactions in the read list.
	WriteCollusion(tid, proc_index);
	return 1;
}
