/*
 * translist.c
 *
 *  Created on: Dec 1, 2015
 *      Author: xiaoxin
 */
#include<pthread.h>
#include<stdlib.h>
#include<malloc.h>

#include "type.h"
#include "translist.h"
#include "trans.h"
#include "data.h"
#include "thread_global.h"
#include "socket.h"
#include "mem.h"

TransactionId* ReadTransTable[TABLENUM][READLISTMAX];

TransactionId* WriteTransTable[TABLENUM];
//TransactionId WriteTransTable[TABLENUM][WRITELISTTABLEMAX];

//pthread_spinlock_t ReadTransLock[TABLENUM][RECORDNUM];
//pthread_spinlock_t WriteTransLock[TABLENUM][RECORDNUM];

static void InitTransactionListMem();

void InitReadListMemAlloc(void)
{
	TransactionId* ReadList=NULL;
	TransactionId* OldReadList=NULL;

	char* memstart;
	Size size;
	THREAD* threadinfo;

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	memstart=threadinfo->memstart;

	size=sizeof(TransactionId)*(NODENUM*THREADNUM+1);

	ReadList=(TransactionId*)MemAlloc((void*)memstart, size);

	if(ReadList==NULL)
	{
		printf("memory allocation error for read-list.\n");
		exit(-1);
	}

	pthread_setspecific(NewReadListKey, ReadList);

	OldReadList=(TransactionId*)MemAlloc((void*)memstart, size);

	if(OldReadList==NULL)
	{
		printf("memory allocation error for read-list.\n");
		exit(-1);
	}

	pthread_setspecific(OldReadListKey, OldReadList);

	memset((char*)OldReadList, 0, size);

}

void InitReadListMem(void)
{
	TransactionId* ReadList=NULL;
	TransactionId* OldReadList=NULL;

	Size size;

	size=sizeof(TransactionId)*(NODENUM*THREADNUM+1);

	//printf("before get\n");
	ReadList=(TransactionId*)pthread_getspecific(NewReadListKey);

	//OldReadList=(TransactionId*)pthread_getspecific(OldReadListKey);

	//memcpy((char*)OldReadList, (char*)ReadList, size);
	//printf("after get %d %d\n", ReadList, size);
	memset((char*)ReadList,0,size);
	//printf("read-list end\n");
}

void InitTransactionList(void)
{
   int i, j, k;

   InitTransactionListMem();
   for (i = 0; i < TABLENUM; i++)
   {
      for (j = 0; j < RecordNum[i]; j++)
      {
    	  WriteTransTable[i][j]=InvalidTransactionId;
      }
   }



   for (i = 0; i < TABLENUM; i++)
   {
      for (j = 0; j < READLISTMAX; j++)
      {
         for(k = 0; k < RecordNum[i]; k++)
         {
             ReadTransTable[i][j][k]= InvalidTransactionId;
         }
      }
   }
   /*
   for (i = 0; i < TABLENUM; i++)
   {
      for (j = 0; j < READLISTTABLEMAX; j++)
      {
         pthread_spin_init(&(ReadTransLock[i][j]), PTHREAD_PROCESS_PRIVATE);
      }
   }

   for (i = 0; i < TABLENUM; i++)
   {
      for (j = 0; j < WRITELISTTABLEMAX; j++)
      {
         pthread_spin_init(&(WriteTransLock[i][j]), PTHREAD_PROCESS_PRIVATE);
      }
   }
   */
}

/*
 * input:'index' for thread id, tid for transaction ID.
 */
void ReadListInsert(int tableid, int h, TransactionId tid, int index)
{
   //pthread_spin_lock(&ReadTransLock[tableid][h]);
	ReadTransTable[tableid][index][h] = tid;
   //pthread_spin_unlock(&ReadTransLock[tableid][h]);
}

/* test is use to test if position have a read transaction, note that the read list lock should be get in the outer loop */
/* the function is not used in the distributed system instead by the copy mechanism*/
TransactionId ReadListRead(int tableid, int h, int test)
{
	return ReadTransTable[tableid][test][h];
}

/*
 * 'index' for the thread ID(location).
 */
void ReadListDelete(int tableid, int h, int index)
{
   //pthread_spin_lock(&ReadTransLock[tableid][h]);
   ReadTransTable[tableid][index][h] = InvalidTransactionId;
   //pthread_spin_unlock(&ReadTransLock[tableid][h]);
}

/*
 * input:'tid' for transaction ID, 'index' for current transaction's threadID.
 */
void WriteListInsert(int tableid, int h, TransactionId tid)
{
   //pthread_spin_lock(&WriteTransLock[tableid][h]);
   /*
   WriteTransTable[tableid][h].transactionid = tid;
   WriteTransTable[tableid][h].index=index;
   */
   //WriteTransTable[tableid][h].index = t.threadid;
   WriteTransTable[tableid][h]=tid;
   //pthread_spin_unlock(&WriteTransLock[tableid][h]);
}

/*
WriteTransListNode* WriteListRead(int tableid, int h)
{
	WriteTransListNode wr_tid;

	wr_tid.transactionid=WriteTransTable[tableid][h].transactionid;
	if(TransactionIdIsValid(wr_tid.transactionid))
		 wr_tid.index=WriteTransTable[tableid][h].index;
	else wr_tid.index=-1;
	return &wr_tid;
}
*/
TransactionId WriteListRead(int tableid, int h)
{
	/*
   if (WriteTransTable[tableid][h][test] != 0)
	   return WriteTransTable[tableid][h][test];
   else
	   return InvalidTransactionId;
	   */
	return WriteTransTable[tableid][h];
}

/*
 * no need function.
 * get the index of write-transaction according to the write-tid.
 */
int WriteListReadindex(int tableid, int h)
{
	/*
   if (WriteTransTable[tableid][h][test] != 0)
	   return WriteTransTable[tableid][h][test];
   else
	   return InvalidTransactionId;
	   */
	return WriteTransTable[tableid][h];
}

/*
void WriteListDelete(int tableid, int h)
{
   pthread_spin_lock(&WriteTransLock[tableid][h]);
   WriteTransTable[tableid][h].transactionid = 0;
   WriteTransTable[tableid][h].index = -1;
   pthread_spin_unlock(&WriteTransLock[tableid][h]);
}
*/
void WriteListDelete(int tableid, int h)
{
   //pthread_spin_lock(&WriteTransLock[tableid][h]);
   WriteTransTable[tableid][h] = InvalidTransactionId;
   //pthread_spin_unlock(&WriteTransLock[tableid][h]);
}

void InitTransactionListMem()
{
	int i, j;

	for(i=0;i<TABLENUM;i++)
	{
		//WriteTransTable[i]=(WriteTransListNode*)malloc(sizeof(WriteTransListNode)*RecordNum[i]);
		WriteTransTable[i]=(TransactionId*)malloc(sizeof(TransactionId)*RecordNum[i]);
		if(WriteTransTable[i]==NULL)
		{
			printf("malloc error for Write TransTable. %d\n",i);
			exit(-1);
		}
		for(j=0;j<READLISTMAX;j++)
		{
			ReadTransTable[i][j]=(TransactionId*)malloc(sizeof(TransactionId)*RecordNum[i]);
			//WriteTransTable[i][j]=(TransactionId*)malloc(sizeof(TransactionId)*RecordNum[i]);
			if(ReadTransTable[i][j]==NULL)
			{
				printf("malloc error for Read TransTable.%d %d\n",i,j);
				exit(-1);
			}
		}
	}
}

void MergeReadList(uint64_t* buffer)
{
	TransactionId* ReadList=NULL;
	TransactionId* OldReadList=NULL;

	int i;
	TransactionId rdtid;

	ReadList=(TransactionId*)pthread_getspecific(NewReadListKey);

	OldReadList=(TransactionId*)pthread_getspecific(OldReadListKey);

	for(i=0;i<NODENUM*THREADNUM;i++)
	{
		rdtid = (TransactionId)buffer[i];

		if(rdtid >= OldReadList[i])
		{
			OldReadList[i]=rdtid;
			ReadList[i]=rdtid;
		}
	}

}


