/*
 * mem.c
 *
 *  Created on: Nov 10, 2015
 *      Author: xiaoxin
 */

/*
 *in order to avoid allocating memory space dynamically, we preassign enough memory space for each
 *thread (terminal), when allocating memory space, we just need to let a pointer variable point at
 *the memory address we need.
 */

#include <malloc.h>
#include <stdlib.h>
#include "mem.h"
#include "thread_global.h"
#include "lock_record.h"
#include "trans.h"
#include "data_am.h"
#include "data_record.h"
#include "config.h"

//start address of free memory space for each thread (terminal) at the very beginning.
uint32_t PROC_START_OFFSET=sizeof(PMHEAD);

//start address of memory space changed dynamically for each thread (terminal).
uint32_t ThreadReuseMemStart;

//start address of preassign memory space.
char* MemStart=NULL;

/*
 * compute the value of  ThreadReuseMemStart.
 */
uint32_t ThreadReuseMemStartCompute(void)
{
	uint32_t size=0;

	size+=sizeof(PMHEAD);

	size+=sizeof(THREAD);

	size+=sizeof(TransactionData);

	size+=DataMemSize();

	size+=MaxDataLockNum*sizeof(DataLock);

	//NewReadList
	size+=sizeof(TransactionId)*(NumTerminals+1);

	//OldReadList
	size+=sizeof(TransactionId)*(NumTerminals+1);

	return size;
}

/*
 * allocate the memory needed for all processes ahead, avoid to malloc
 * dynamically during process running.
 */
void InitMem(void)
{
	Size size=MEM_TOTAL_SIZE;

	char* start=NULL;

	ThreadReuseMemStart=ThreadReuseMemStartCompute();

	MemStart=(char*)malloc(size);
	if(MemStart==NULL)
	{
		printf("memory malloc failed.\n");
		exit(-1);
	}
	int procnum;
	PMHEAD* pmhead=NULL;
	for (procnum=0;procnum<MAXPROCS;procnum++)
	{
		start=MemStart+procnum*MEM_PROC_SIZE;
		pmhead=(PMHEAD*)start;
		pmhead->total_size=MEM_PROC_SIZE;
		pmhead->freeoffset=PROC_START_OFFSET;
	}
}
/*
 * new interface for memory allocation in thread running.
 */
void* MemAlloc(void* memstart,Size size)
{
	PMHEAD* pmhead=NULL;
	Size newStart;
	Size newFree;
	void* newSpace;

	pmhead=(PMHEAD*)memstart;

	newStart=pmhead->freeoffset;
	newFree=newStart+size;

	if(newFree>pmhead->total_size)
		newSpace=NULL;
	else
	{
		newSpace=(void*)((char*)memstart+newStart);
		pmhead->freeoffset=newFree;
	}

	if(!newSpace)
	{
		printf("out of memory for process %d\n",pthread_self());
		exit(-1);
	}
	return newSpace;
}

/*
 * new interface for memory clean in process ending.
 */
void MemClean(void *memstart)
{
	PMHEAD* pmhead=NULL;
	Size newStart;
	Size newFree;

	//reset process memory.
	memset((char*)memstart,0,MEM_PROC_SIZE);

	pmhead=(PMHEAD*)memstart;

	pmhead->freeoffset=PROC_START_OFFSET;
	pmhead->total_size=MEM_PROC_SIZE;
}

/*
 * clean transaction memory context.
 * @memstart:start address of current thread's private memory.
 */
void TransactionMemClean(void)
{
	PMHEAD* pmhead=NULL;
	char* reusemem;
	void* memstart;
	THREAD* threadinfo;
	Size size;

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	memstart=(void*)threadinfo->memstart;
	reusemem=(char*)memstart+ThreadReuseMemStart;
	size=MEM_PROC_SIZE-ThreadReuseMemStart;
	memset(reusemem,0,size);

	pmhead=(PMHEAD*)memstart;
	pmhead->freeoffset=ThreadReuseMemStart;

}
