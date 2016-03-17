/*
 * process actions are defined here.
 */
#include <malloc.h>
#include <pthread.h>
#include <stdlib.h>

#include "config.h"
#include "proc.h"
#include "mem.h"
#include "thread_global.h"
#include "trans.h"
#include "lock.h"
#include "util.h"

PROCHEAD* prohd;

//start address of process array.
PROC* procbase;

int UpdateProcStartId(int index,CommitId cid);

int UpdateProcCommitId(int index,StartId sid);

CommitId GetTransactionCidMin(int index);

StartId GetTransactionSidMin(int index);

 StartId GetTransactionSidMax(int index);

StartId GetTransactionSid(int index);

int IsPairConflict(int index, CommitId cid);

void AtEnd_ProcArray(int index);

void SetProcAbort(int index);

inline bool IsTransactionActive(int index, TransactionId tid, StartId* sid, CommitId* cid);

int ForceUpdateProcSidMax(int index, CommitId cid);

int ForceUpdateProcCidMin(int index, StartId sid);

int MVCCUpdateProcId(int index, StartId sid_min, CommitId cid_min);


void InitProc(void)
{
	Size size;
	int i;
	PROC* proc;

	//initialize the process array information.
	prohd=(PROCHEAD*)malloc(sizeof(PROCHEAD));
	prohd->maxprocs=MAXPROCS;
	prohd->numprocs=0;

	//initialize the process array.
	size=ProcArraySize();
	procbase=(PROC*)malloc(size);

	if(procbase==NULL)
	{
		printf("memory alloc failed for procarray.\n");
		return;
	}

	memset((char*)procbase,0,ProcArraySize());

	for(i=0;i<MAXPROCS;i++)
	{
		proc=(PROC*)((char*)procbase+i*sizeof(PROC));
		proc->index=i;

		proc->complete=0;
	}
}

/*
 * entrance of each thread (terminal) when starting.
 */
void *ProcStart(void* args)
{
	int i;
	char* start=NULL;
	THREAD* threadinfo;

	Size size;

	terminalArgs* param=(terminalArgs*)args;

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

	threadinfo->index=i;
	threadinfo->memstart=(char*)start;

	//initialize the transaction ID assignment for per thread.
	ProcTransactionIdAssign(threadinfo);

	InitRandomSeed();

	//memory allocation for each transaction data-structure.
	InitTransactionStructMemAlloc();

	//start running transactions here.
	TransactionRunSchedule(args);

	return NULL;
}

Size ProcArraySize(void)
{
	return sizeof(PROC)*MAXPROCS;
}

/*
 * Is 'cid' conflict with the transaction's time interval by index.
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

	//add lock to access.
	//pthread_spin_lock(&ProcArrayElemLock[index]);
	conflict=(cid > proc->sid_min)?0:1;
	//pthread_spin_unlock(&ProcArrayElemLock[index]);

	return conflict;
}

/*
 * when transaction T1 is invisible to T2, and T1 commits before
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
	proc=(PROC*)((char*)procbase+offset);

	if(proc->tid == InvalidTransactionId)
	{
		//just skip the invalid transaction.
		return 1;
	}
	pthread_spin_lock(&ProcArrayElemLock[index]);
	if(proc->complete > 0)
	{
		if(proc->sid_max < cid)
		{
			//just skip it.
			pthread_spin_unlock(&ProcArrayElemLock[index]);
			return 1;
		}
		else
		{
	        //return to determine the [S,C] again.
			pthread_spin_unlock(&ProcArrayElemLock[index]);

			return 0;
		}
	}

	if(cid > proc->sid_min)
	{
		proc->sid_max = ((cid-1) < proc->sid_max) ? (cid-1) : proc->sid_max;
	}
	else
	{
		//current transaction has to rollback.
		pthread_spin_unlock(&ProcArrayElemLock[index]);
		return 0;
	}
	pthread_spin_unlock(&ProcArrayElemLock[index]);
	return 1;

}

/* function: when T1 is invisible to T2, and T1 commits first, there must be:
 * T2's StartId < T1's CommitId, so T2's StartId should be changed by itself.
 * @return: '0' to rollback, '1' to continue.
 */
int ForceUpdateProcSidMax(int index, CommitId cid)
{
	PROC* proc;
	Size offset;

	offset=index*sizeof(PROC);

	//to hold lock here.
	proc=(PROC*)((char*)procbase+offset);

	if(proc->sid_min >= cid)
	{
		return 0;
	}

	pthread_spin_lock(&ProcArrayElemLock[index]);

	proc->sid_max=(proc->sid_max > cid) ? cid : proc->sid_max;

	pthread_spin_unlock(&ProcArrayElemLock[index]);


	return 1;
}

/*
 * function: when T1 is invisible to T2, and T2 commits first, there must be:
 * T2's StartId < T1's CommitId, so T1's CommitId should be changed by itself.
 */
int ForceUpdateProcCidMin(int index, StartId sid)
{
	PROC* proc;
	Size offset;

	offset=index*sizeof(PROC);

	//to hold lock here.
	proc=(PROC*)((char*)procbase+offset);

	//add lock to access.
	pthread_spin_lock(&ProcArrayElemLock[index]);

	proc->cid_min = (sid > proc->cid_min) ? sid : proc->cid_min;

	pthread_spin_unlock(&ProcArrayElemLock[index]);

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

	if(proc->tid == InvalidTransactionId)
	{
		//just skip the invalid transaction.
		return 1;
	}
	pthread_spin_lock(&ProcArrayElemLock[index]);
	if(proc->complete > 0)
	{
		if(proc->cid > sid)
		{
			pthread_spin_unlock(&ProcArrayElemLock[index]);
			return 1;
		}
		else
		{
			pthread_spin_unlock(&ProcArrayElemLock[index]);

		    //return to determine [S,C] again.
			return 0;
		}
	}

	proc->cid_min = (sid > proc->cid_min) ? sid : proc->cid_min;

	pthread_spin_unlock(&ProcArrayElemLock[index]);

	return 1;

}

/*
 * get the cid_min by the 'index'.
 */
CommitId GetTransactionCidMin(int index)
{
	CommitId cid;

	cid=(procbase+index)->cid_min;

	return cid;
}

StartId GetTransactionSidMin(int index)
{
	StartId sid_min;

	sid_min=(procbase+index)->sid_min;

	if(sid_min == InvalidTransactionId)
		sid_min=-1;

	return sid_min;
}

StartId GetTransactionSidMax(int index)
{
	StartId sid_max;

	sid_max=(procbase+index)->sid_max;

	return sid_max;
}

/*
 * return the 'sid' to determine the 'cid'.
 */
StartId GetTransactionSid(int index)
{
	StartId sid;

	//add lock to access.
	pthread_spin_lock(&ProcArrayElemLock[index]);

	if((procbase+index)->tid == InvalidTransactionId)
		sid=-1;
	else if((procbase+index)->sid_max != MAXINTVALUE)
		sid=(procbase+index)->sid_max;
	else
		sid=(procbase+index)->sid_min+5;

	pthread_spin_unlock(&ProcArrayElemLock[index]);

	return sid;
}

/*
 * clean the process array at the end of transaction by index.
 */
void AtEnd_ProcArray(int index)
{
	PROC* proc;
	proc=procbase+index;

	//add lock to access.
	pthread_spin_lock(&ProcArrayElemLock[index]);

	proc->tid=InvalidTransactionId;

	proc->cid_min=0;
	proc->sid_min=0;
	proc->sid_max=MAXINTVALUE;

	proc->cid=0;
	proc->complete=0;

	pthread_spin_unlock(&ProcArrayElemLock[index]);
}

/*
 * function: set the process to status 'abort'.
 */
void SetProcAbort(int index)
{
	PROC* proc;
	proc=procbase+index;

	proc->tid=InvalidTransactionId;
}


/*
 * function: to see whether the transaction by 'tid' is still active.
 * @return:'true' for active, 'false' for committed or aborted.
 */
bool IsTransactionActive(int index, TransactionId tid, StartId* sid, CommitId* cid)
{
	//return false;
	PROC* proc;
	proc=&(procbase[index]);

	if(proc->tid == tid)
	{
		pthread_spin_lock(&ProcArrayElemLock[index]);
		if(proc->complete > 0)
		{
			/*
			 * to consider here.
			 * the 'cid' and 'sid' may be changed during committing.
			 */
			*sid=proc->sid_min;
			*cid=proc->cid;
		}
		pthread_spin_unlock(&ProcArrayElemLock[index]);
		return true;
	}
	else
	{
		return false;
	}

}

/*
 * function: update transaction's time interval after reading a data item.
 */
int MVCCUpdateProcId(int index, StartId sid_min, CommitId cid_min)
{
	PROC* proc;
	proc=(PROC*)(procbase+index);

	if(proc->sid_min < sid_min)
		proc->sid_min=sid_min;

	if(proc->cid_min < cid_min)
		proc->cid_min=cid_min;

	return 0;
}
