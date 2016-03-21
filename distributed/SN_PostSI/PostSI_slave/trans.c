/*
 * trans.c
 *
 *  Created on: Nov 10, 2015
 *      Author: xiaoxin
 */
/*
 * transaction actions are defined here.
 */
#include<malloc.h>
#include<unistd.h>
#include"config.h"
#include"trans.h"
#include"thread_global.h"
#include"trans_conflict.h"
#include"data_record.h"
#include"lock_record.h"
#include"mem.h"
#include"proc.h"
#include"lock.h"
#include"data_am.h"
#include"transactions.h"
#include"socket.h"
#include"translist.h"
#include"communicate.h"

TransactionId thread_0_tid;

static IDMGR* CentIdMgr;

StartId AssignTransactionStartId(TransactionId tid);

CommitId AssignTransactionCommitId(TransactionId tid);

int ConfirmIdAssign(StartId* sid, CommitId* cid);

void InitTransactionStructMemAlloc(void);

/*
 * no need function.
 * to make sure the transaction-id match the pthread's 'index'.
 */
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

	CentIdMgr->curid=nodeid*THREADNUM*MaxTransId + 1;
}

void ProcTransactionIdAssign(THREAD* thread)
{
	int index;
	index=thread->index;

	thread->maxid=(index+1)*MaxTransId;
}

TransactionId AssignTransactionId(void)
{
	TransactionId tid;

	THREAD* threadinfo;

	threadinfo=pthread_getspecific(ThreadInfoKey);

	if(threadinfo->curid<=threadinfo->maxid)
		tid=threadinfo->curid++;
	else
		return 0;
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

	/* to set data memory. */
	InitDataMemAlloc();

	/* to set data-lock memory. */
	InitDataLockMemAlloc();

	/* to set read-list memory. */
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
	int lindex;

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	memstart=threadinfo->memstart;
	index=threadinfo->index;

	lindex=GetLocalIndex(index);

	size=sizeof(TransactionData);

	td=(TransactionData*)MemAlloc((void*)memstart,size);
	if(td==NULL)
	{
		printf("memalloc error.\n");
		return;
	}

	pthread_setspecific(TransactionDataKey,td);

	/* to set data memory. */
	InitDataMem();
	/* to set data-lock memory. */
	InitDataLockMem();

	/* to set read-list memory. */
	InitReadListMem();

	/* assign transaction ID here. */
	td->tid=AssignTransactionId();

	if(!TransactionIdIsValid(td->tid))
	{
		printf("transaction ID assign error.\n");
		return;
	}

	/* transaction ID assignment succeeds. */
	td->sid_min=0;
	td->cid_min=0;
	td->sid_max=MAXINTVALUE;

	proc=(PROC*)((char*)procbase+lindex*sizeof(PROC));

	/* to hold lock here. */
	pthread_spin_lock(&ProcArrayElemLock[lindex]);
	proc->pid=pthread_self();
	proc->tid=td->tid;
	proc->sid_min=0;
	proc->sid_max=MAXINTVALUE;
	proc->cid_min=0;
	pthread_spin_unlock(&ProcArrayElemLock[lindex]);
}

int CommitTransaction(void)
{
	StartId sid;
	CommitId cid;
	TransactionId tid;
	TransactionData* tdata;
	THREAD* threadinfo;
	int index, lindex;
	int result;
        bool confirm = true;
	tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
	tid=tdata->tid;

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index=threadinfo->index;

	lindex=GetLocalIndex(index);

	sid=AssignTransactionStartId(tid);

	cid=AssignTransactionCommitId(tid);

	if(!ConfirmIdAssign(&sid, &cid))
	{
		confirm=false;
	}
	
	/* transaction can commit. */
	if(confirm && !CommitInvisibleUpdate(index,sid,cid))
	{
		/* to make sure that cid > sid. */
		if(cid==sid)cid+=1;
		CommitDataRecord(tid,cid);
		result=0;
	}
	/* transaction has to roll back. */
	else
	{
		SetProcAbort(lindex);

		AbortDataRecord(tid, -1);
		result=-2;
	}

	DataLockRelease();

	AtEnd_InvisibleTable(index);

	/* by here, we consider that the transaction committed successfully or abort successfully. */
	AtEnd_ProcArray(index);

	/* clean the transaaction's memory. */
	TransactionMemClean();

	return result;
}

void AbortTransaction(int trulynum)
{
	TransactionId tid;
	TransactionData* tdata;
	THREAD* threadinfo;
	int index;
	int lindex;

	tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
	tid=tdata->tid;

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index=threadinfo->index;

	lindex=GetLocalIndex(index);

	SetProcAbort(lindex);

	AbortDataRecord(tid,trulynum);

	DataLockRelease();

	/* reset the row by 'index' of invisible-table */
	AtEnd_InvisibleTable(index);

	/* by here, we consider that the transaction abort successfully. */
	AtEnd_ProcArray(index);

	/* clean the transaaction's memory. */
	TransactionMemClean();
}

void ReleaseDataConnect(void)
{
	if (Send1(0, nodeid, cmd_release) == -1)
		printf("release data connect server send error\n");
}

void ReleaseConnect(void)
{
	int i;
	THREAD* threadinfo;
	int index;

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index=threadinfo->index;
	int lindex;
	lindex = GetLocalIndex(index);
	for (i = 0; i < nodenum; i++)
	{
		if (Send1(lindex, i, cmd_release) == -1)
			printf("release connect server %d send error\n", i);
	}
}

void TransactionRunSchedule(void* args)
{
   /* to run transactions according to args. */
   int type;

   terminalArgs* param=(terminalArgs*)args;
   type=param->type;

   THREAD* threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);

   if(type==0)
   {
      printf("begin LoadData......\n");
	  LoadData();
	  thread_0_tid=threadinfo->curid;
	  ResetMem(0);
	  ResetProc();
	  ReleaseDataConnect();
   }
   else
   {
      printf("begin execute transactions...\n");
	  executeTransactions(transactionsPerTerminal, param->whse_id, param->dist_id, param->StateInfo);
	  ReleaseConnect();
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
 * sid_min is only possibly changed in it's private thread.
 */
StartId AssignTransactionStartId(TransactionId tid)
{
	StartId sid;
	THREAD* threadinfo;
	int index;
	int lindex;

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);

	index=threadinfo->index;

	lindex=GetLocalIndex(index);

	sid=GetTransactionSidMin(lindex);

	return sid;
}

CommitId AssignTransactionCommitId(TransactionId tid)
{
	CommitId cid;
	CommitId cid_min;
	THREAD* threadinfo;
	int index;
	int lindex;

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index=threadinfo->index;

	lindex=GetLocalIndex(index);

	cid_min=GetTransactionCidMin(lindex);
    cid=GetTransactionCid(index,cid_min);

	return cid;
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
 *@return:'1' for success, '-1' for rollback.
 *@input:'index':record the truly done data-record's number when PreCommit failed.
 */
int PreCommit(int* index)
{
	char* DataMemStart, *start;
	int num,i,result;
	DataRecord* ptr;
	TransactionData* tdata;
	TransactionId tid;
	THREAD* threadinfo;
	int proc_index;

	tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
	tid=tdata->tid;

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	proc_index=threadinfo->index;

	DataMemStart=(char*)pthread_getspecific(DataMemKey);
	start=DataMemStart+DataNumSize;

	num=*(int*)DataMemStart;

	/* sort the data-operation records. */
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
			result=TrulyDataInsert(ptr->table_id, ptr->index, ptr->tuple_id, ptr->value, ptr->node_id);
			break;
		case DataUpdate:
			result=TrulyDataUpdate(ptr->table_id, ptr->index, ptr->tuple_id, ptr->value, ptr->node_id);
			break;
		case DataDelete:
			result=TrulyDataDelete(ptr->table_id, ptr->index, ptr->tuple_id, ptr->node_id);
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

	WriteCollusion(tid, proc_index);
	return 1;
}

int GetNodeId(int index)
{
   return (index/THREADNUM);
}

int GetLocalIndex(int index)
{
	return (index%THREADNUM);
}

int ConfirmIdAssign(StartId* sid, CommitId* cid)
{
	THREAD* threadinfo;
	int index;
	int lindex;
	PROC* proc;

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index=threadinfo->index;

	lindex=GetLocalIndex(index);

	proc=(PROC*)(procbase+lindex);

	pthread_spin_lock(&ProcArrayElemLock[lindex]);
	if(*sid > proc->sid_max)
	{
		pthread_spin_unlock(&ProcArrayElemLock[lindex]);
		return 0;
	}

	*cid=(proc->cid_min > (*cid))?proc->cid_min:(*cid);

	*cid=((*sid+1) > *cid) ? (*sid+1) : *cid;

	proc->cid=*cid;
	proc->complete=1;

	pthread_spin_unlock(&ProcArrayElemLock[lindex]);

	return 1;
}
