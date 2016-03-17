/*
 * data_record.c
 *
 *  Created on: Dec 7, 2015
 *      Author: xiaoxin
 */
#include <assert.h>
#include "config.h"
#include "data_record.h"
#include "mem.h"
#include "thread_global.h"
#include "data_am.h"
#include "trans.h"

/*
 * we should record the transaction's information on tuples updated when committing, functions 'CommitInsertData',
 * 'CommitUpdatetData', 'CommitDeleteData' are like this.
 */

void CommitInsertData(void * data, int table_id, int index, TimeStampTz ctime)
{
	THash HashTable;

	HashTable=TableList[table_id];

	Record * r = (Record *) &HashTable[index];

    pthread_spin_lock(&RecordLatch[table_id][index]);
	r->lcommit = (r->lcommit + 1) % VERSIONMAX;
	r->VersionList[r->lcommit].committime = 0;
	pthread_spin_unlock(&RecordLatch[table_id][index]);
}

void CommitUpdateData(void * data, int table_id, int index, TimeStampTz ctime)
{
	THash HashTable;

	HashTable=TableList[table_id];

	Record * r = (Record *) &HashTable[index];

    pthread_spin_lock(&RecordLatch[table_id][index]);
	r->lcommit = (r->lcommit + 1) % VERSIONMAX;
	r->VersionList[r->lcommit].committime = 0;
    pthread_spin_unlock(&RecordLatch[table_id][index]);
}

void CommitDeleteData(void * data, int table_id, int index, TimeStampTz ctime)
{
	THash HashTable;

	HashTable=TableList[table_id];

	Record * r = (Record *) &HashTable[index];

    pthread_spin_lock(&RecordLatch[table_id][index]);
	r->lcommit = (r->lcommit + 1) % VERSIONMAX;
	r->VersionList[r->lcommit].committime = 0;

	pthread_spin_unlock(&RecordLatch[table_id][index]);
}

/*
 * functions 'AbortInsertData', 'AbortUpdatetData', 'AbortDeleteData' erase the updates on tuples when
 * the transaction aborts.
 */

void AbortInsertData(void * data, int table_id, int index)
{
	VersionId newest;
	THash HashTable;

	HashTable=TableList[table_id];

	Record * r = (Record *) &HashTable[index];

	pthread_spin_lock(&RecordLatch[table_id][index]);
	newest = (r->rear + VERSIONMAX -1) % VERSIONMAX;

	r->tupleid=InvalidTupleId;
	r->rear=0;
	r->front=0;
	r->lcommit=-1;

	r->VersionList[newest].tid = InvalidTransactionId;

	r->VersionList[newest].value = 0;

	pthread_spin_unlock(&RecordLatch[table_id][index]);
}

void AbortUpdateData(void * data, int table_id, int index)
{
	VersionId newest;
	THash HashTable;

	HashTable=TableList[table_id];

	Record * r = (Record *) &HashTable[index];

	pthread_spin_lock(&RecordLatch[table_id][index]);
	newest = (r->rear + VERSIONMAX -1) % VERSIONMAX;
	r->VersionList[newest].tid =InvalidTransactionId;
	r->rear = newest;
	pthread_spin_unlock(&RecordLatch[table_id][index]);
}

void AbortDeleteData(void * data, int table_id, int index)
{
	VersionId newest;
	THash HashTable;

	HashTable=TableList[table_id];

	Record * r = (Record *) &HashTable[index];

	pthread_spin_lock(&RecordLatch[table_id][index]);
	newest = (r->rear + VERSIONMAX -1) % VERSIONMAX;
	r->VersionList[newest].tid = InvalidTransactionId;
	r->VersionList[newest].deleted = false;
	r->rear = newest;
	pthread_spin_unlock(&RecordLatch[table_id][index]);
}

/*
 * function: append one data-update-record.
 */
void DataRecordInsert(DataRecord* datard)
{
	int num;
	char* start;
	char* DataMemStart;
	DataRecord* ptr;

	//get the thread's pointer to data memory.
	DataMemStart=(char*)pthread_getspecific(DataMemKey);

	start=(char*)((char*)DataMemStart+DataNumSize);
	num=*(int*)DataMemStart;

	if(DataNumSize+(num+1)*sizeof(DataRecord) > DataMemSize())
	{
		printf("data memory out of space. PID: %lu\n",pthread_self());
		return;
	}

	*(int*)DataMemStart=num+1;

	//start address for record to insert.
	start=start+num*sizeof(DataRecord);

	//insert the data record here.
	ptr=(DataRecord*)start;
	ptr->type=datard->type;
	ptr->data=datard->data;

	ptr->table_id=datard->table_id;
	ptr->tuple_id=datard->tuple_id;

	ptr->value=datard->value;

	ptr->index=datard->index;
}

Size DataMemSize(void)
{
	return DataMenMaxSize;
}

/*
 * function: memory allocation for data-update-records.
 */
void InitDataMemAlloc(void)
{
	char* DataMemStart=NULL;
	char* memstart;
	THREAD* threadinfo;
	Size size;

	//get start address of current thread's memory.
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	memstart=threadinfo->memstart;

	size=DataMemSize();
	DataMemStart=(char*)MemAlloc((void*)memstart,size);

	if(DataMemStart==NULL)
	{
		printf("thread memory allocation error for data memory.PID:%lu\n",pthread_self());
		return;
	}

	//set global variable.
	pthread_setspecific(DataMemKey,DataMemStart);
}

void InitDataMem(void)
{
	char* DataMemStart=NULL;

	DataMemStart=(char*)pthread_getspecific(DataMemKey);

	memset(DataMemStart,0,DataMemSize());
}

/*
 * write the commit time of current transaction to every updated tuple.
 */
void CommitDataRecord(TransactionId tid, TimeStampTz ctime)
{
	int num;
	int i;
	char* DataMemStart=NULL;
	char* start;
	DataRecord* ptr;
	void* data=NULL;
	int table_id;
	uint64_t index;

	DataMemStart=(char*)pthread_getspecific(DataMemKey);
	start=DataMemStart+DataNumSize;

	num=*(int*)DataMemStart;

	//deal with data-update record one by one.
	for(i=0;i<num;i++)
	{
		ptr=(DataRecord*)(start+i*sizeof(DataRecord));
		data=ptr->data;
		table_id=ptr->table_id;
		index=ptr->index;
		switch(ptr->type)
		{
		case DataInsert:
			//get the address of inserted tuple and write the cid.
			//then update the tailer.
			CommitInsertData(data,table_id, index, ctime);
			break;
		case DataUpdate:
			CommitUpdateData(data,table_id, index, ctime);
			break;
		case DataDelete:
			//get the address of deleted tuple and write the cid.
			//then update the tailer.
			CommitDeleteData(data,table_id, index, ctime);
			break;
		default:
			printf("shouldn't arrive here.\n");
		}
	}
}

/*
 * rollback all updated tuples by current transaction.
 */
void AbortDataRecord(TransactionId tid, int trulynum)
{
	int num;
	int i;
	char* DataMemStart=NULL;
	char* start;
	DataRecord* ptr;
	void* data=NULL;
	int table_id;
	uint64_t index;

	DataMemStart=(char*)pthread_getspecific(DataMemKey);
	start=DataMemStart+DataNumSize;

	if(trulynum==-1)
		num=*(int*)DataMemStart;
	//transaction abort in function AbortTransaction.
	else
		num=trulynum;

	for(i=0;i<num;i++)
	{
		ptr=(DataRecord*)(start+i*sizeof(DataRecord));
		data=ptr->data;
		table_id=ptr->table_id;
		index=ptr->index;
		switch(ptr->type)
		{
		case DataInsert:
			//get the address of inserted tuple and free it's space.
			AbortInsertData(data, table_id, index);
			break;
		case DataUpdate:
			//get the address of inserted and deleted tuple, and free the
			//space of inserted, reset the space of deleted to original.
			AbortUpdateData(data, table_id, index);
			break;
		case DataDelete:
			//get the address of deleted tuple and reset the space to
			//original.
			AbortDeleteData(data, table_id, index);
			break;
		default:
			printf("shouldn't get here.\n");
		}
	}
}

/*
 * function: sort the transaction's data-update-records to avoid dead lock between concurrent
 * update transactions.
 * @input:'dr':the start address of data-record, 'num': number of data-record.
 */
void DataRecordSort(DataRecord* dr, int num)
{
	//sort according to the table_id and tuple_id;
	DataRecord* ptr1, *ptr2;
	DataRecord* startptr=dr;
	DataRecord temp;
	int i,j;

	for(i=0;i<num-1;i++)
		for(j=0;j<num-i-1;j++)
		{
			ptr1=startptr+j;
			ptr2=startptr+j+1;
			if((ptr1->table_id > ptr2->table_id) || ((ptr1->table_id == ptr2->table_id) && (ptr1->tuple_id > ptr2->tuple_id)))
			{
				temp.data=ptr1->data;
				temp.table_id=ptr1->table_id;
				temp.tuple_id=ptr1->tuple_id;
				temp.type=ptr1->type;
				temp.value=ptr1->value;
				temp.index=ptr1->index;

				ptr1->data=ptr2->data;
				ptr1->table_id=ptr2->table_id;
				ptr1->tuple_id=ptr2->tuple_id;
				ptr1->type=ptr2->type;
				ptr1->value=ptr2->value;
				ptr1->index=ptr2->index;

				ptr2->data=temp.data;
				ptr2->table_id=temp.table_id;
				ptr2->tuple_id=temp.tuple_id;
				ptr2->type=temp.type;
				ptr2->value=temp.value;
				ptr2->index=temp.index;
			}
		}
}

/*
 * function: to see whether the data item has been updated by current transaction.
 * @return:'1' for visible inner own transaction, '-1' for invisible inner own transaction,
 * '0' for first access the tuple inner own transaction.
 */
TupleId IsDataRecordVisible(char* DataMemStart, int table_id, TupleId tuple_id)
{
	int num,i;
	char* start=DataMemStart+DataNumSize;
	num=*(int*)DataMemStart;
	DataRecord* ptr;

	for(i=num-1;i>=0;i--)
	{
		ptr=(DataRecord*)(start+i*sizeof(DataRecord));
		if(ptr->tuple_id == tuple_id && ptr->table_id == table_id)
		{
			if(ptr->type == DataDelete)
			{
				//the tuple has been deleted by current transaction.
				return -1;
			}
			else
			{
				//insert or update.
				return ptr->value;
			}
		}
	}

	//first access the tuple.
	return 0;
}



