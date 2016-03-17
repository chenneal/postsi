/*
 * snapshot.c
 *
 *  Created on: Dec 7, 2015
 *      Author: xiaoxin
 */
#include "snapshot.h"
#include "thread_global.h"
#include "thread_main.h"
#include "proc.h"
#include "trans.h"
#include "type.h"
#include "lock.h"
#include "mem.h"
#include "config.h"

/*
 * memory allocation for transaction snapshot in each thread (terminal).
 */
void InitTransactionSnapshotDataMemAlloc(void)
{
	Snapshot* snap;
	THREAD* threadinfo;
	char* memstart;
	Size size;

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	memstart=threadinfo->memstart;

	size=SnapshotSize();

	snap=(Snapshot*)MemAlloc((void*)memstart,size);

	if(snap == NULL)
	{
		printf("snapshot memory allocation error.\n");
		return;
	}

	size=sizeof(TransactionId)*MAXPROCS;
	snap->tid_array=(TransactionId*)MemAlloc((void*)memstart,size);
	if(snap->tid_array == NULL)
	{
		printf("snapshot array allocation error.\n");
		return;
	}

	pthread_setspecific(SnapshotDataKey,snap);
}

void InitTransactionSnapshotData(void)
{
	Snapshot* snap;
	Size size;


	snap=(Snapshot*)pthread_getspecific(SnapshotDataKey);

	snap->tid_min=0;

	snap->tid_max=0;
	snap->tcount=0;

	size=sizeof(TransactionId)*MAXPROCS;

	memset((char*)snap->tid_array,0,size);
}

Size SnapshotSize(void)
{
	return sizeof(Snapshot);
}

/*
 * get the transactions snapshot at the beginning of each transaction.
 */
void GetTransactionSnapshot(void)
{
	PROC* proc;
	Snapshot* snap;
	TransactionId tid_min;
	TransactionId tid_max;
	TransactionId tid;
	THREAD* threadinfo;
	int count;
	int i;
	int index;

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index=threadinfo->index;

	snap=(Snapshot*)pthread_getspecific(SnapshotDataKey);

	//add read-lock here on ProcArray.
	AcquireWrLock(&ProcArrayLock, LOCK_SHARED);

	tid_max=CentIdMgr->latestcompletedId;
	tid_max+=1;

	tid_min=tid_max;
	count=0;

	for(i=0;i<=NumTerminals;i++)
	{
		proc=procbase+i;
		tid=proc->tid;
		if(TransactionIdIsValid(tid) && i != index)
		{
			if(tid < tid_min)
				tid_min=tid;

			snap->tid_array[count++]=tid;
		}
	}

	ReleaseWrLock(&ProcArrayLock);

	snap->tcount=count;
	snap->tid_min=tid_min;
	snap->tid_max=tid_max;
}

/*
 * function: to see whether the transaction by 'tid' is in the snapshot 'snap'.
 * @return:'true' for still in running, 'false' for already completed.
 */
bool TidInSnapshot(TransactionId tid, Snapshot* snap)
{
	int i;
	//transaction committed or abort.
	if(tid < snap->tid_min)
		return false;

	//transaction still in running.
	if(tid > snap->tid_max)
		return true;

	for(i=0;i<snap->tcount;i++)
	{
		if(tid == snap->tid_array[i])
			return true;
	}
	return false;
}

