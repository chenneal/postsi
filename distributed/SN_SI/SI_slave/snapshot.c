/*
 * snapshot.c
 *
 *  Created on: Dec 7, 2015
 *      Author: xiaoxin
 */
#include"snapshot.h"
#include"thread_global.h"
#include"thread_main.h"
#include"proc.h"
#include"trans.h"
#include"type.h"
#include"lock.h"
#include"mem.h"
#include"config.h"

//static Size SnapshotSize(void);

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
 * @return:'true' for still in running, 'false' for already completed.
 */
bool TidInSnapshot(TransactionId tid, uint64_t * tid_array, int count, int min, int max)
{
	int i;
	/* transaction committed or abort. */
	if(tid < min)
		return false;

	/* transaction still in running. */
	if(tid > max)
		return true;

	for(i=0;i<count;i++)
	{
		if(tid == (TransactionId)tid_array[i])
			return true;
	}
	return false;
}
