/*
 * translist.c
 *
 *  Created on: Dec 1, 2015
 *      Author: xiaoxin
 */
#include <pthread.h>
#include <malloc.h>

#include "type.h"
#include "data_am.h"
#include "translist.h"
#include "trans.h"
#include "config.h"
#include "trans_conflict.h"
#include "thread_global.h"
#include "mem.h"

//visitor(readers) list for every data item.
TransactionId* ReadTransTable[TABLENUM][READLISTMAX];

//visitor(writer) list for every data item.
TransactionId* WriteTransTable[TABLENUM];

inline void ReadListInsert(int tableid, int h, TransactionId tid, int index);
TransactionId ReadListRead(int tableid, int h, int index);
void ReadListDelete(int tableid, int h, int index);
void WriteListInsert(int tableid, int h, TransactionId tid);
TransactionId WriteListRead(int tableid, int h);
void WriteListDelete(int tableid, int h, int index);

void WriteCollusion(TransactionId tid, int index);

void MergeReadList(int tableid, int h);

int ReadCollusion(int tableid, int h, TransactionId tid, int index);

static void InitTransactionListMem(void);

/*
 * function: memory allocation for visitor(conflict transactions reading the same data item) list of each thread(terminal).
 */
void InitReadListMemAlloc(void)
{
	TransactionId* ReadList=NULL;
	TransactionId* OldReadList=NULL;

	char* memstart;
	Size size;
	THREAD* threadinfo;

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	memstart=threadinfo->memstart;

	size=sizeof(TransactionId)*(NumTerminals+1);

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

	size=sizeof(TransactionId)*(NumTerminals+1);

	ReadList=(TransactionId*)pthread_getspecific(NewReadListKey);

	memset((char*)ReadList,0,size);
}

void InitTransactionList(void)
{
   int i, j, k;
   InitTransactionListMem();

   for (i = 0; i < TABLENUM; i++)
   {
      for (j = 0; j < READLISTMAX; j++)
      {
         for(k = 0; k < RecordNum[i]; k++)
         {
             ReadTransTable[i][j][k]= 0;
         }
      }
   }

   for(i=0;i<TABLENUM;i++)
	   for(j=0;j<RecordNum[i];j++)
	   {
		   WriteTransTable[i][j]=InvalidTransactionId;
	   }
}

/*
 * input:'index' for thread id, tid for transaction ID.
 */
void ReadListInsert(int tableid, int h, TransactionId tid, int index)
{
   ReadTransTable[tableid][index][h] = tid;
}

/*
 * function: read from the visitor(readers) list.
 * @input: 'index' is the thread-ID.
 */
TransactionId ReadListRead(int tableid, int h, int index)
{
	return ReadTransTable[tableid][index][h];
}

/*
 * function: delete in the visitor(readers) list.
 */
void ReadListDelete(int tableid, int h, int index)
{
   ReadTransTable[tableid][index][h] = 0;
}

/*
 * @input:'tid' for transaction ID, 'index' for current transaction's threadID.
 */
void WriteListInsert(int tableid, int h, TransactionId tid)
{
	WriteTransTable[tableid][h]=tid;
}

TransactionId WriteListRead(int tableid, int h)
{
	return WriteTransTable[tableid][h];
}

void WriteListDelete(int tableid, int h, int index)
{
   WriteTransTable[tableid][h]=InvalidTransactionId;
}

/*
 * function: allocate memory space for visitors (readers and writers) list of each data item.
 */
void InitTransactionListMem(void)
{
	int i, j;

	for(i=0;i<TABLENUM;i++)
	{
		WriteTransTable[i]=(TransactionId*)malloc(sizeof(TransactionId)*RecordNum[i]);
		if(WriteTransTable[i]==NULL)
		{
			printf("malloc error for Write TransTable. %d\n",i);
			exit(-1);
		}
		for(j=0;j<READLISTMAX;j++)
		{
			ReadTransTable[i][j]=(TransactionId*)malloc(sizeof(TransactionId)*RecordNum[i]);
			if(ReadTransTable[i][j]==NULL)
			{
				printf("malloc error for Read TransTable.%d %d\n",i,j);
				exit(-1);
			}
		}
	}
}

/*
 * function: record read-write anti-dependency between current transaction(writer) and other conflict
 * transactions read the same data item.
 */
void WriteCollusion(TransactionId tid, int index)
{
	int i;
	TransactionId* ReadList=NULL;

	TransactionId rdtid;

	CommitId cid=0;
	StartId sid;

	ReadList=(TransactionId*)pthread_getspecific(NewReadListKey);

	ReadList[index]=InvalidTransactionId;

	for(i=0;i<=NumTerminals;i++)
	{
		 rdtid=ReadList[i];

		 if (TransactionIdIsValid(rdtid) && IsTransactionActive(i,rdtid,&sid,&cid))
		 {
			  if(cid > 0)
			  {
				  //adjust cid_min of current transaction according to the 'sid'.
				  ForceUpdateProcCidMin(index, sid);
			  }
			  else
			  {
				  InvisibleTableInsert(index, i);
			  }
		 }
	}
}

/*
 * function: merge the visitor(readers) list of the data items updated by current transaction.
 */
void MergeReadList(int tableid, int h)
{
	TransactionId* ReadList=NULL;
	TransactionId* OldReadList=NULL;

	int i;
	TransactionId rdtid;

	ReadList=(TransactionId*)pthread_getspecific(NewReadListKey);

	OldReadList=(TransactionId*)pthread_getspecific(OldReadListKey);

	for(i=0;i<=NumTerminals;i++)
	{
		rdtid = ReadTransTable[tableid][i][h];

		if(rdtid >= OldReadList[i])
		{
			OldReadList[i]=rdtid;
			ReadList[i]=rdtid;
		}
	}

}

/*
 * function: record read-write anti-dependency between current transaction(reader) and
 * conflict transaction updating the same data item.
 */
int ReadCollusion(int tableid, int h, TransactionId tid, int index)
{
   int  wr_index, result;
   TransactionId wrtid;
   CommitId cid=0;
   StartId sid;

   TransactionId* OldReadList=(TransactionId*)pthread_getspecific(OldReadListKey);

   wrtid=WriteTransTable[tableid][h];
   wr_index=(wrtid-1)/MaxTransId;

   if(TransactionIdIsValid(wrtid) && (wr_index != index) && IsTransactionActive(wr_index,wrtid,&sid,&cid))
   {
	   if(cid > 0)
	   {
		   //adjust sid_max of current transaction according to the 'cid'.
		   result=ForceUpdateProcSidMax(index, cid);
		   return result;
	   }
	   else
	   {
		   InvisibleTableInsert(wr_index, index);
	   }

   }

   return 1;
}

