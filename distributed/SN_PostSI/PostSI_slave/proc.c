/*
 * proc.c
 *
 *  Created on: 2015-11-9
 *      Author: DELL
 */
/*
 * process actions are defined here.
 */
#include<malloc.h>
#include<pthread.h>
#include<stdlib.h>
#include<sys/shm.h>
#include<sys/socket.h>
#include "config.h"
#include "type.h"
#include "proc.h"
#include "mem.h"
#include "thread_global.h"
#include "trans.h"
#include "lock.h"
#include "socket.h"
#include "communicate.h"
#include "util.h"

PROCHEAD* prohd;
int proc_shmid;
PROC* procbase;

//pointer to process in committing.
PROCCOMMIT* proccommit;

//Proc information should be stored in the shared memory.
void InitProc(void)
{
	Size size;
	int i;
	PROC* proc;
	//initialize the process array information.
	prohd=(PROCHEAD*)malloc(sizeof(PROCHEAD));
	prohd->maxprocs=THREADNUM;
	prohd->numprocs=0;

	//initialize the 'proccommit'.
	//proccommit=(PROCCOMMIT*)malloc(sizeof(PROCCOMMIT));
	//proccommit->index=-1;
	//proccommit->tid=InvalidTransactionId;

	//initialize the process array.
	size=ProcArraySize();
	proc_shmid = shmget(IPC_PRIVATE, size, SHM_MODE);
	if (proc_shmid == -1)
	{
		printf("proc shmget error.\n");
		return;
	}
	procbase=(PROC*)shmat(proc_shmid, 0, 0);
	if (procbase == (PROC*)-1)
	{
		printf("proc shmat error.\n");
		return;
	}

	memset((char*)procbase,0,ProcArraySize());

	for(i=0;i<THREADNUM;i++)
	{
		proc=(PROC*)((char*)procbase+i*sizeof(PROC));
		proc->index=i;
	}
}

void *ProcStart(void* args)
{
	int i;
	int j;
	char* start=NULL;
	THREAD* threadinfo;

	int type;
	//IDMGR* ProcIdMgr;

	Size size;

	terminalArgs* param=(terminalArgs*)args;

	type=param->type;

	pthread_mutex_lock(&prohd->ilock);
	i=prohd->numprocs++;
	pthread_mutex_unlock(&prohd->ilock);

	start=(char*)MemStart+MEM_PROC_SIZE*i;

	size=sizeof(THREAD);

	threadinfo=(THREAD*)MemAlloc((void*)start,size);

	if(threadinfo==NULL)
	{
		printf("memory alloc error during process running.\n");
		exit(-1);
	}

	pthread_setspecific(ThreadInfoKey,threadinfo);

	threadinfo->index= nodeid*THREADNUM+i;
	threadinfo->memstart=(char*)start;

	if(type==1 && i ==0)
		threadinfo->curid=thread_0_tid+1;
	else
		threadinfo->curid=threadinfo->index*MaxTransId+1;

	//initialize the transaction ID assignment for per thread.
	ProcTransactionIdAssign(threadinfo);

	InitRandomSeed();

	InitTransactionStructMemAlloc();

	if (type == 1)
	{
       for (j = 0; j < NODENUM; j++)
       {
	      InitClient(j, i);
       }
	}
	else
	{
		InitClient(nodeid, i);
	}

	//start running transactions here.
	TransactionRunSchedule(args);
	//printf("PID:%lu start.\n",pthread_self());

	return NULL;
}
void ProcArrayAdd(void)
{

}

Size ProcArraySize(void)
{
	return sizeof(PROC)*THREADNUM;
}

/*
 * Is 'cid' conflict with the tansaction by index.
 * @return:'1':conflict, '0':not conflict.
 */
int IsPairConflict(int index, CommitId cid)
{
	PROC* proc;
	Size offset;
	int conflict;

	offset=index*sizeof(PROC);

	//to hold lock here.
	proc=(PROC*)((char*)procbase+offset);

	//return (cid > proc->sid_min)?0:1;
	//add lock to access.
	pthread_spin_lock(&ProcArrayElemLock[index]);
	conflict=(cid > proc->sid_min)?0:1;
	pthread_spin_unlock(&ProcArrayElemLock[index]);

	return conflict;
}
/*
 * when transaction T1 is invisible to T2, and T1 commit before
 * T2, then upon T1 committing, it will update T2's StartId.
 * @index: T2's location index in process array.
 * @cid:T1's commit ID.
 * @return: return 0 to rollback, else continue.
 */
int UpdateProcStartId(int index,CommitId cid)
{
	PROC* proc;
	Size offset;

	offset=index*sizeof(PROC);

	//to hold lock here.
	//proc=(PROC*)((char*)procbase+offset);
	proc=(PROC*)(procbase+index);	

	//add lock to access.
	//pthread_spin_lock(&ProcArrayElemLock[index]);
	if(cid > proc->sid_min)
	{
		proc->sid_max = ((cid-1) < proc->sid_max) ? (cid-1) : proc->sid_max;
	}
	else
	{
		//current transaction has to rollback.
		//pthread_spin_unlock(&ProcArrayElemLock[index]);
		//printf("cid=%d, sid_min=%d, sid_max=%d, cid_min=%d, index=%d\n", cid, proc->sid_min, proc->sid_max, proc->cid_min, index);
		return 0;
	}
	//pthread_spin_unlock(&ProcArrayElemLock[index]);

	return 1;

}

/*
 * when transaction T1 is invisible to T2, and T2 commit before
 * T1, then upon T2 committing, it will update T1's CommitId.
 * @index:T1's location index in process array.
 * @StartId:T2's Start ID.
 */
int UpdateProcCommitId(int index,StartId sid)
{
	PROC* proc;
	Size offset;

	offset=index*sizeof(PROC);

	//to hold lock here.
	proc=(PROC*)((char*)procbase+offset);

	//add lock to access.
	//pthread_spin_lock(&ProcArrayElemLock[index]);

	proc->cid_min = (sid > proc->cid_min) ? sid : proc->cid_min;

	//pthread_spin_unlock(&ProcArrayElemLock[index]);

	return 0;

}

/*
 * update the thread's 'sid_min' and 'cid_min' after reading a data.
 */
int AtRead_UpdateProcId(int index, StartId sid_min)
{
	PROC* proc;
	Size offset;

	offset=index*sizeof(PROC);

	//to hold lock here.
	proc=(PROC*)((char*)procbase+offset);

	//add lock to access.
	pthread_spin_lock(&ProcArrayElemLock[index]);

	proc->sid_min=sid_min;

	//update the 'cid_min'.
	if(proc->cid_min < sid_min)
		proc->cid_min = sid_min;
	pthread_spin_unlock(&ProcArrayElemLock[index]);
	return 0;
}

/*
 * get the cid_min by the index.
 */
CommitId GetTransactionCidMin(int index)
{
	CommitId cid;

	//add lock to access.
	//pthread_spin_lock(&ProcArrayElemLock[index]);
	cid=(procbase+index)->cid_min;
	//pthread_spin_unlock(&ProcArrayElemLock[index]);

	return cid;
}

StartId GetTransactionSidMin(int index)
{
	StartId sid_min;

	//add lock to access.
	//pthread_spin_lock(&ProcArrayElemLock[index]);
	sid_min=(procbase+index)->sid_min;
	//pthread_spin_unlock(&ProcArrayElemLock[index]);
	return sid_min;
}

StartId GetTransactionSidMax(int index)
{
	StartId sid_max;

	//add lock to access.
	pthread_spin_lock(&ProcArrayElemLock[index]);
	sid_max=(procbase+index)->sid_max;
	pthread_spin_unlock(&ProcArrayElemLock[index]);

	return sid_max;
}

/*
 * clean the process array at the end of transaction by index.
 */
void AtEnd_ProcArray(int index)
{
	int lindex;

	lindex=GetLocalIndex(index);

	PROC* proc;
	proc=(PROC*)(procbase+lindex);

	//add lock to access.
	pthread_spin_lock(&ProcArrayElemLock[lindex]);

	proc->cid_min=0;
	proc->sid_min=0;
	proc->sid_max=MAXINTVALUE;
	proc->tid=InvalidTransactionId;

	proc->cid=0;
	proc->complete=0;

	pthread_spin_unlock(&ProcArrayElemLock[lindex]);
}

/*
 * to see whether the transaction by 'tid' is still active.
 * @return:'true' for active, 'false' for committed or aborted.
 */
//bool IsTransactionActive(int index, int windex, int rindex, TransactionId tid, TransactionId wtid, TransactionId rtid)
bool IsTransactionActive(int index, TransactionId tid, bool IsRead, StartId* sid, CommitId* cid)
{
	int status;
	int lindex;
	int nid;

	TransactionData* tdata;
	THREAD* threadinfo;

	TransactionId self_tid;
	int self_index;

	uint64_t* buffer;

	tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);

	self_tid=tdata->tid;
	self_index=threadinfo->index;

	lindex = GetLocalIndex(self_index);
	// find the index of the transaction to test.
    nid = GetNodeId(index);
	//if (Send7(lindex, nid, cmd_collisioninsert, self_index, index, self_tid, tid, IsRead) == -1)
    //printf("index = %d, selfindex = %d, lindex = %d, nid = %d, tid = %d, slef_tid\n", index, self_index, lindex, nid, tid, self_tid);
    if (Send6(lindex, nid, cmd_collisioninsert, self_index, index, self_tid, tid, IsRead) == -1)
		printf("insert collision send error\n");
	if (Recv(lindex, nid, 3) == -1)
		printf("insert collision recv error\n");

	buffer=(uint64_t*)recv_buffer[lindex];

	status=(int)buffer[0];
	*sid=(TransactionId)buffer[1];
	*cid=(TransactionId)buffer[2];

	if(status==0)
		return false;
	else
		return true;

	/*
	status = *(recv_buffer[lindex]);
	if (status == 0) return false;
	else return true;
	*/

}

/*
 * @return: '0' to rollback, '1' to continue.
 */
int ForceUpdateProcSidMax(int index, CommitId cid)
{
	PROC* proc;
	Size offset;

	int lindex;

	lindex=GetLocalIndex(index);

	offset=lindex*sizeof(PROC);

	//to hold lock here.
	proc=(PROC*)((char*)procbase+offset);

	//add lock to access.
	//pthread_spin_lock(&ProcArrayElemLock[index]);

	if(proc->sid_min >= cid)
	{
		//pthread_spin_unlock(&ProcArrayElemLock[index]);
		//printf("ForceUpdateProcSidMax: sid_min=%d, sid_max=%d, cid_min=%d, cid=%d\n", proc->sid_min, proc->sid_max, proc->cid_min, cid);
		return 0;
	}

	//pthread_spin_lock(&ProcArrayElemLock[index]);

	proc->sid_max=(proc->sid_max > cid) ? cid : proc->sid_max;

	//pthread_spin_unlock(&ProcArrayElemLock[index]);


	return 1;
}

/*
 * @return: '0' to abort, '1' to go ahead.
 */
int MVCCUpdateProcId(int index, StartId sid_min, CommitId cid_min)
{
	PROC* proc;
	proc=(PROC*)(procbase+index);

	if(proc->sid_min < sid_min)
		proc->sid_min=sid_min;

	if(proc->cid_min < cid_min)
		proc->cid_min=cid_min;

	if(proc->sid_min > proc->sid_max)
       {
	//printf("sid_min = %d, sid_max = %d\n", proc->sid_min, proc->sid_max);
	return 0;
       }

	return 1;
}

int ForceUpdateProcCidMin(int index, StartId sid)
{
	PROC* proc;

	int lindex;

	lindex=GetLocalIndex(index);

	proc=(PROC*)(procbase+lindex);
	//add lock to access.
	//pthread_spin_lock(&ProcArrayElemLock[index]);

	proc->cid_min = (sid > proc->cid_min) ? sid : proc->cid_min;

	//pthread_spin_unlock(&ProcArrayElemLock[index]);

	return 1;
}

void SetProcAbort(int index)
{
	PROC* proc;
	proc=(PROC*)(procbase+index);

	//add lock to access.
	//pthread_spin_lock(&ProcArrayElemLock[index]);

	proc->tid=InvalidTransactionId;

	//pthread_spin_unlock(&ProcArrayElemLock[index]);
}

void ResetProc(void)
{
	prohd->maxprocs=THREADNUM;
	prohd->numprocs=0;
}
