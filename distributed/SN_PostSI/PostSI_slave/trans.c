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

	//if(thread->curid  index*MaxTransId+1
	//thread->curid=index*MaxTransId+1;
	thread->maxid=(index+1)*MaxTransId;
	/*
	pthread_mutex_lock(&CentIdMgr->IdLock);

	thread->curid=CentIdMgr->curid;
	thread->maxid=thread->curid+MaxTransId-1;

	CentIdMgr->curid+=MaxTransId;

	pthread_mutex_unlock(&CentIdMgr->IdLock);
	*/
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

	int* ThreadSidMin;

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

	ThreadSidMin=(int*)malloc(sizeof(int));

	if(ThreadSidMin==NULL)
	{
		printf("ThreadSidMin allocation error\n");
		exit(-1);
	}

	*ThreadSidMin=0;
	pthread_setspecific(SidMinKey, ThreadSidMin);
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

	int* ThreadSidMin;

	ThreadSidMin=(int*)pthread_getspecific(SidMinKey);

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

	//to set data memory.
	InitDataMem();
	//to set data-lock memory.
	InitDataLockMem();

	//to set read-list memory.
	InitReadListMem();

	//assign transaction ID here.
	td->tid=AssignTransactionId();
	//printf("StartTransaction: PID:%lu TID:%d index:%d\n",pthread_self(),td->tid,threadinfo->index);
	if(!TransactionIdIsValid(td->tid))
	{
		printf("transaction ID assign error.\n");
		return;
	}

	//transaction ID assignment succeeds.
	td->sid_min=0;
	td->cid_min=0;
	td->sid_max=MAXINTVALUE;

	proc=(PROC*)((char*)procbase+lindex*sizeof(PROC));

	//to hold lock here.
	//initialize those fields of process.
	//add lock to access.
	//printf("StartTransaction: test1 index=%d.\n",index);
	pthread_spin_lock(&ProcArrayElemLock[lindex]);
	//printf("StartTransaction: test2 .\n");
	proc->pid=pthread_self();
	proc->tid=td->tid;
	proc->sid_min=0;
	//proc->sid_min=*ThreadSidMin;
	proc->sid_max=MAXINTVALUE;
	proc->cid_min=0;
	pthread_spin_unlock(&ProcArrayElemLock[lindex]);
	//printf("StartTransaction: TID:%d finished.\n",td->tid);
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

	bool confirm=true;

	int* ThreadSidMin;

	ThreadSidMin=(int*)pthread_getspecific(SidMinKey);

	tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
	tid=tdata->tid;

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index=threadinfo->index;

	lindex=GetLocalIndex(index);

	sid=AssignTransactionStartId(tid);

	//add lock on ProcArray.
	//AcquireWrLock(&CommitProcArrayLock,LOCK_EXCLUSIVE);

	//record current transaction's information in 'proccommit'.
	//AcquireWrLock(&ProcCommitLock,LOCK_EXCLUSIVE);
	//proccommit->tid=tid;
	//proccommit->index=index;
	//ReleaseWrLock(&ProcCommitLock);

	//add lock on row by 'index' of invisible-table.
	//prevent other transaction updating the row by 'index' of invisible-table.
	//AcquireWrLock(&InvisibleArrayRowLock[index],LOCK_EXCLUSIVE);

	cid=AssignTransactionCommitId(tid);

	if(!ConfirmIdAssign(&sid, &cid))
	{
		//printf("TID:%d, sid:%d, cid:%d\n",tid, sid, cid);
		confirm=false;
	}

	//printf("PID:%u SID:%d CID:%d \n",pthread_self(),sid,cid);
	//transaction can commit.
	if(confirm && !CommitInvisibleUpdate(index,sid,cid))
	{
		//TransactionContextCommit(tid,cid);
		//printf("CommitTransaction: TID: %d commit.\n",tid);
		//to make sure that cid > sid.
		if(cid==sid)cid+=1;
		CommitDataRecord(tid,cid);
		result=0;

		if(*ThreadSidMin < cid)
			*ThreadSidMin=cid;
		//DataLockRelease();
	}
	//transaction has to rollback.
	else
	{
		//TransactionContextAbort(tid);
		//printf("CommitTransaction: TID: %d abort.\n",tid);
		SetProcAbort(lindex);

		AbortDataRecord(tid, -1);
		result=-2;
		//DataLockRelease();

		//reset the row by 'index' of invisible-table/
		//AtEnd_InvisibleTable(index);
	}

	DataLockRelease();

	AtEnd_InvisibleTable(index);
	//AtEnd_ProcArray(index);

	//by here, we consider that the transaction committed successfully or abort successfully.
	AtEnd_ProcArray(index);

	//release the lock on row by 'index' of invisible-table.
	//ReleaseWrLock(&InvisibleArrayRowLock[index]);

	//clean the process in committing.
	//AcquireWrLock(&ProcCommitLock,LOCK_EXCLUSIVE);
	//proccommit->tid=InvalidTransactionId;
	//proccommit->index=-1;
	//ReleaseWrLock(&ProcCommitLock);

	//release lock on ProcArray.
	//ReleaseWrLock(&CommitProcArrayLock);

	//printf("CommitTransaction: TID: %d finished.\n",tid);
	//clean the transaaction's memory.
	TransactionMemClean();

	return result;
}

/*
 * context of transaction, transId identifies the context of
 * the transaction to be executed.
 */
void RunTransaction(int transId)
{

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

	//add lock on ProcArray.
	//AcquireWrLock(&CommitProcArrayLock,LOCK_EXCLUSIVE);

	//record current transaction's information in 'proccommit'.
	//AcquireWrLock(&ProcCommitLock,LOCK_EXCLUSIVE);
	//proccommit->tid=tid;
	//proccommit->index=index;
	//ReleaseWrLock(&ProcCommitLock);

	//add lock on row by 'index' of invisible-table.
	//prevent other transaction updating the row by 'index' of invisible-table.
	//AcquireWrLock(&InvisibleArrayRowLock[index],LOCK_EXCLUSIVE);
	SetProcAbort(lindex);

	AbortDataRecord(tid,trulynum);

	DataLockRelease();

	//reset the row by 'index' of invisible-table/
	AtEnd_InvisibleTable(index);

	//by here, we consider that the transaction abort successfully.
	AtEnd_ProcArray(index);

	//release the lock on row by 'index' of invisible-table.
	//ReleaseWrLock(&InvisibleArrayRowLock[index]);

	//clean the process in committing.
	//AcquireWrLock(&ProcCommitLock,LOCK_EXCLUSIVE);
	//proccommit->tid=InvalidTransactionId;
	//proccommit->index=-1;
	//ReleaseWrLock(&ProcCommitLock);

	//release lock on ProcArray.
	//ReleaseWrLock(&CommitProcArrayLock);

	//clean the transaaction's memory.
	TransactionMemClean();
	//AtEnd_InvisibleTable(index);
	//AtEnd_ProcArray(index);

	//TransactionContextAbort(tid);
}

/*
 *  the next functions is wrote by yu to run a different bench on our database system
 */

/*
void Judge(int judge)
{
   if (judge == -1) AbortTransaction(-1);
   else if (judge == 0) printf("not found!\n");
   else if (judge == 1) CommitTransaction();
   else printf("wrong!\n");
}

void ComplexTransaction(int operator, int tableid, int tupleid)
{
	int flag;
	printf("operator:%d,tableid:%d,tupleid:%d\n",operator,tableid,tupleid);
	switch (operator)
	{
		case 1: { Data_Insert(tableid, tupleid, 1, 1); break; }
		case 2: { Data_Delete(tableid, tupleid, 1); break; }
		case 3: { Data_Update(tableid, tupleid, 1, 1); break; }
		case 4: { Data_Read(tableid, tupleid, 1, &flag); break; }
		default: break;
	}
}

int RandomTableid()
{
	//srand((unsigned) time(NULL));
	return (rand() % 9);
}

int RandomTupleid()
{
	//srand((unsigned) time(NULL));
	return (rand() % 100 + 1);
}

int RandomOperator()
{
	//srand((unsigned) time(NULL));
	return (rand() % 3 + 2);
}

void execTransaction()
{
	int j = 0;
	int result;
	int index;
	do
	{
		int i;
		StartTransaction();
		for (i=0; i<5; i++)
		{
		   //ComplexTransaction(3, 1, 4);
		   //ComplexTransaction(2, 1, 4);
		   //ComplexTransaction(3, 1, 4);
			ComplexTransaction(RandomOperator(),RandomTableid(), RandomTupleid());
		}
		result=PreCommit(&index);
		if(result == 1)
		{
			CommitTransaction();
		}
		else
		{
			//current transaction has to rollback.
			//to do here.
			//we should know from which DataRecord to rollback.
			AbortTransaction(index);
		}
		j++;
	} while (j < 100000);

}
*/
/*
void TransactionRunSchedule(void* args)
{
	int index;
	int * i = (int *) args;
	int j;
	int result;
	int flag;
	if (*i == 1)
	{
		for (j = 0; j < 20; j++)
		{
	      StartTransaction();
	      Data_Insert(2, j+1, j+1, nodeid);
	      result=PreCommit(&index);
	      if(result == 1)
	      {
	      	CommitTransaction();
	      }
	      else
	      {
	      	  //current transaction has to rollback.
	      	  //to do here.
	      	  //we should know from which DataRecord to rollback.
	      	  AbortTransaction(index);
	      }
		}
	}

	if (*i == 2)
	{
		sleep(3);
		for (j = 0; j < 1000; j++)
		{
	      StartTransaction();
	      Data_Update(2, j%20+1, j+6, nodeid);
	      Data_Update(2, (j+1)%20+1, j+6, (nodeid+1)%NODENUM);
	      Data_Update(2, (j+2)%20+1, j+6, (nodeid+2)%NODENUM);
	      Data_Update(2, (j+3)%20+1, j+6, (nodeid+3)%NODENUM);
	      Data_Read(2, (j+2)%20+1, (nodeid+4)%NODENUM, &flag);
	      result=PreCommit(&index);
	      if(result == 1)
	      {
	      	CommitTransaction();
	      }
	      else
	      {
	      	  //current transaction has to rollback.
	      	  //to do here.
	      	  //we should know from which DataRecord to rollback.
	      	  AbortTransaction(index);
	      }
		}
	}

	if (*i == 3)
	{
		sleep(3);
		for (j = 0; j < 1000; j++)
		{
	      StartTransaction();
	      Data_Update(2, j%20+1, j+6, nodeid);
	      Data_Update(2, (j+1)%20+1, j+6, (nodeid+1)%NODENUM);
	      Data_Update(2, (j+2)%20+1, j+6, (nodeid+2)%NODENUM);
	      Data_Update(2, (j+3)%20+1, j+6, (nodeid+3)%NODENUM);
	      Data_Read(2, (j+2)%20+1, (nodeid+4)%NODENUM, &flag);
	      result=PreCommit(&index);
	      if(result == 1)
	      {
	      	CommitTransaction();
	      }
	      else
	      {
	      	  //current transaction has to rollback.
	      	  //to do here.
	      	  //we should know from which DataRecord to rollback.
	      	  AbortTransaction(index);
	      }
		}
	}


	if (*i == 4)
	{
		sleep(3);
		for (j = 0; j < 1000; j++)
		{
	      StartTransaction();
	      Data_Update(2, j%20+1, j+6, nodeid);
	      Data_Update(2, (j+1)%20+1, j+6, (nodeid+1)%NODENUM);
	      Data_Update(2, (j+2)%20+1, j+6, (nodeid+2)%NODENUM);
	      Data_Update(2, (j+3)%20+1, j+6, (nodeid+3)%NODENUM);
	      Data_Read(2, (j+2)%20+1, (nodeid+4)%NODENUM, &flag);
	      result=PreCommit(&index);
	      if(result == 1)
	      {
	      	CommitTransaction();
	      }
	      else
	      {
	      	  //current transaction has to rollback.
	      	  //to do here.
	      	  //we should know from which DataRecord to rollback.
	      	  AbortTransaction(index);
	      }
		}
	}
}
*/

void ReleaseDataConnect(void)
{
	if (Send1(0, nodeid, cmd_release) == -1)
		printf("release data connect server %d send error\n");
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
	//to run transactions according to args.
	int i,result;
	int index, type;
	terminalArgs* param=(terminalArgs*)args;
	type=param->type;
	THREAD* threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);

	if(type==0)
	{
		printf("begin LoadData......\n");
		//getchar();
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

/*
	StartTransaction();
	Data_Update(0,2);
	result=PreCommit(&index);
	if(result == 1)
	{
		CommitTransaction();
	}
	else
	{
		//current transaction has to rollback.
		//to do here.
		//we should know from which DataRecord to rollback.
		AbortTransaction(index);
	}
	StartTransaction();
	Data_Update(0,2);
	Data_Update(0,4);
	Data_Insert(0,8);
	//Data_Delete(0,2);
	Data_Update(0,2);
	result=PreCommit(&index);
	if(result == 1)
	{
		CommitTransaction();
	}
	else
	{
		//current transaction has to rollback.
		//to do here.
		//we should know from which DataRecord to rollback.
		AbortTransaction(index);
	}
	*/
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
	/*
	TransactionData* tdata;

	tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);

	sid=tdata->sid_min;
	*/
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
	//to hold lock here.

	cid_min=GetTransactionCidMin(lindex);
    cid=GetTransactionCid(index,cid_min);

    //printf("PID:%u %d %d\n",pthread_self(),cid_min,cid);
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
	//AbortDataRecord(tid);
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

	//sort the data-operation records.
	DataRecordSort((DataRecord*)start, num);
/*
	for(i=0;i<num;i++)
	{
		ptr=(DataRecord*)(start+i*sizeof(DataRecord));
		printf("%d %d %ld %d %ld\n",i,ptr->table_id,ptr->tuple_id,ptr->type, ptr->value);
	}
*/
	//printf("PreCommit: TID:%d sort finished.\n",tdata->tid);

	for(i=0;i<num;i++)
	{
		ptr=(DataRecord*)(start+i*sizeof(DataRecord));

		switch(ptr->type)
		{

		case DataInsert:
			//printf("PreCommit: insert %d %ld.\n",ptr->table_id,ptr->tuple_id);
			//printf("tableid = %d, tupleid = %ld, index = %d, value = %ld, type = %d\n", ptr->table_id, ptr->tuple_id, ptr->index, ptr->value, ptr->type);
			if(ptr->tuple_id <= 0)
			{
				printf("error.\n");
				exit(-1);
			}
			result=TrulyDataInsert(ptr->table_id, ptr->index, ptr->tuple_id, ptr->value, ptr->node_id);
			//printf("PreCommit: after insert.\n");
			break;
		case DataUpdate:
			//printf("PreCommit: TID: %d update %d %ld.\n", tdata->tid, ptr->table_id,ptr->tuple_id);
			result=TrulyDataUpdate(ptr->table_id, ptr->index, ptr->tuple_id, ptr->value, ptr->node_id);
			//printf("PreCommit: after update.\n");
			break;
		case DataDelete:
			//printf("PreCommit: delete %d %ld.\n",ptr->table_id,ptr->tuple_id);
			result=TrulyDataDelete(ptr->table_id, ptr->index, ptr->tuple_id, ptr->node_id);
			//printf("PreCommit: after delete.\n");
			break;
		default:
			printf("PreCommit:shouldn't arrive here.\n");
		}
		if(result == -1)
		{
			//return to rollback.
			//printf("PreCommit: TID: %d rollback.\n",tdata->tid);
			*index=i;
			return -1;
		}
	}

	WriteCollusion(tid, proc_index);
	//printf("PreCommit: TID: %d finished.\n",tdata->tid);
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
		//abort current transaction.
		//printf("TID:%d, sid:%d, sid_min:%d, sid_max:%d\n", proc->tid, *sid, proc->sid_min, proc->sid_max);
		pthread_spin_unlock(&ProcArrayElemLock[lindex]);
		return 0;
	}

	*cid=(proc->cid_min > (*cid))?proc->cid_min:(*cid);
	//assert(*cid >= *sid);

	*cid=((*sid+1) > *cid) ? (*sid+1) : *cid;

	proc->cid=*cid;
	proc->complete=1;

	pthread_spin_unlock(&ProcArrayElemLock[lindex]);

	return 1;
}
