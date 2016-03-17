/*
 * proc.c
 *
 *  Created on: 2015��11��9��
 *      Author: DELL
 */
/*
 * process actions are defined here.
 */
#include <malloc.h>
#include <pthread.h>
#include <stdlib.h>

#include "proc.h"
#include "mem.h"
#include "thread_global.h"
#include "trans.h"
#include "lock.h"
#include "util.h"

PROCHEAD* prohd;

//start address of process array.
PROC* procbase;

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
	}
}

void ResetProc(void)
{
	prohd->maxprocs=MAXPROCS;
	prohd->numprocs=0;
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
 * clean the process array at the end of transaction by index.
 */
void AtEnd_ProcArray(int index)
{
	//return;
	PROC* proc;
	TransactionId tid;
	proc=procbase+index;
	tid=proc->tid;

	AcquireWrLock(&ProcArrayLock, LOCK_EXCLUSIVE);

	proc->tid=InvalidTransactionId;
	proc->pid=0;

	if(CentIdMgr->latestcompletedId < tid)
		CentIdMgr->latestcompletedId = tid;

	ReleaseWrLock(&ProcArrayLock);
}

/*
 * add to the process array when one transaction begins.
 */
void ProcArrayAdd(int index)
{
	PROC* proc;
	TransactionData* td;

	td=(TransactionData*)pthread_getspecific(TransactionDataKey);
	proc=procbase+index;

	AcquireWrLock(&ProcArrayLock, LOCK_EXCLUSIVE);

	proc->tid=td->tid;
	proc->pid=pthread_self();

	ReleaseWrLock(&ProcArrayLock);
}

/*
 * to see whether the transaction by 'tid' is still active.
 * @return:'true' for active, 'false' for committed or aborted.
 */
bool IsTransactionActive(int index, TransactionId tid)
{
	PROC* proc;
	proc=procbase+index;

	//to hold lock here.
	return (proc->tid == tid) ? true : false;

}
