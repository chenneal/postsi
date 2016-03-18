/*
 * lock.c
 *
 *  Created on: Dec 2, 2015
 *      Author: xiaoxin
 */
/*
 * interface to operations about locks on ProcArray and InvisibleTable.
 */
#include<malloc.h>
#include"lock.h"
#include"socket.h"

//lock to access 'proccommit'.
pthread_rwlock_t ProcCommitLock;

//to make sure that at most one transaction commits at the same time.
pthread_rwlock_t CommitProcArrayLock;

//spin-lock is enough.
pthread_spinlock_t * ProcArrayElemLock;

/*
 * initialize the lock on ProcArray and Invisible.
 */

void InitProcLock(void)
{
	int i;
	ProcArrayElemLock = (pthread_spinlock_t *) malloc (THREADNUM * sizeof(pthread_spinlock_t));

	for(i=0;i<THREADNUM;i++)
	{
		pthread_spin_init(&ProcArrayElemLock[i],PTHREAD_PROCESS_SHARED);
	}
}

void InitTransactionLock(void)
{
	pthread_rwlockattr_t attr;
	pthread_rwlockattr_init(&attr);
	pthread_rwlockattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
	//ProcArray
	pthread_rwlock_init(&CommitProcArrayLock, &attr);

	//ProcCommit
	pthread_rwlock_init(&ProcCommitLock, &attr);
}

void InitStorageLock(void)
{
	int i;
	pthread_rwlockattr_t attr;
	pthread_rwlockattr_init(&attr);
	pthread_rwlockattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
	for(i=0;i<MAXPROCS;i++)
	{
		//pthread_rwlock_init(&InvisibleArrayRowLock[i], &attr);
	}
}
/*
 * interface to hold the read-write-lock.
 */
void AcquireWrLock(pthread_rwlock_t* lock, LockMode mode)
{
	if(mode == LOCK_SHARED)
	{
		pthread_rwlock_rdlock(lock);
	}
	else
	{
		pthread_rwlock_wrlock(lock);
	}
}

/*
 * interface to release the read-write-lock.
 */
void ReleaseWrLock(pthread_rwlock_t* lock)
{
	pthread_rwlock_unlock(lock);
}


