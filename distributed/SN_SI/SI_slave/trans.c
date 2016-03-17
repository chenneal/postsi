/*
 * trans.c
 *
 *  Created on: Nov 10, 2015
 *      Author: xiaoxin
 */
/*
 * transaction actions are defined here.
 */
#include<malloc.h>
#include<sys/time.h>
#include<stdlib.h>
#include<assert.h>
#include<sys/socket.h>
#include"trans.h"
#include"thread_global.h"
#include"data_record.h"
#include"lock_record.h"
#include"mem.h"
#include"proc.h"
#include"snapshot.h"
#include"data_am.h"
#include"transactions.h"
#include"config.h"
#include"thread_main.h"
#include"socket.h"
#include"communicate.h"

int PreCommit(int* index);

//get the transaction id, add proc array and get the snapshot from the master node.
void StartTransactionGetData(void)
{
	int index;
	int i;

	int lindex;
	pthread_t pid;
	Snapshot* snap;
	THREAD* threadinfo;
	TransactionData* td;

	td=(TransactionData*)pthread_getspecific(TransactionDataKey);
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	snap=(Snapshot*)pthread_getspecific(SnapshotDataKey);

	int size = 3 + 1 + 1 + MAXPROCS;

	index=threadinfo->index;
	lindex = GetLocalIndex(index);
	pid = pthread_self();

	*(send_buffer[lindex]) = cmd_starttransaction;
	*(send_buffer[lindex]+1) = index;
	*(send_buffer[lindex]+2) = pid;

	if (send(server_socket[lindex], send_buffer[lindex], 3*sizeof(uint64_t), 0) == -1)
		printf("start transaction send error\n");
	if (recv(server_socket[lindex], recv_buffer[lindex], size*sizeof(uint64_t), 0) == -1)
		printf("start transaction recv error\n");

	// get the transaction ID
	td->tid = *(recv_buffer[lindex]+4);

	if(!TransactionIdIsValid(td->tid))
	{
		printf("transaction ID assign error.\n");
		return;
	}

	td->starttime = *(recv_buffer[lindex]+3);
	// get the snapshot
	snap->tcount = *(recv_buffer[lindex]);
	snap->tid_min =*(recv_buffer[lindex]+1);
	snap->tid_max = *(recv_buffer[lindex]+2);

	for (i = 0; i < MAXPROCS; i++)
		snap->tid_array[i] = *(recv_buffer[lindex]+5+i);
}

TimeStampTz GetTransactionStopTimestamp(void)
{
	THREAD* threadinfo;
	int index;
	TransactionData* td;

	td=(TransactionData*)pthread_getspecific(TransactionDataKey);
	// get the pointer to current thread information.
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);

	index=threadinfo->index;
	int lindex;
	lindex = GetLocalIndex(index);

	*(send_buffer[lindex]) = cmd_getendtimestamp;
	if (send(server_socket[lindex], send_buffer[lindex], sizeof(uint64_t), 0) == -1)
	   printf("get end time stamp send error\n");
	if (recv(server_socket[lindex], recv_buffer[lindex], sizeof(uint64_t), 0) == -1)
	   printf("get end time stamp recv error\n");

	td->stoptime = *(recv_buffer[lindex]);
	return (td->stoptime);
}

void UpdateProcArray()
{
   THREAD* threadinfo;
   int index;
   // get the pointer to current thread information.
   threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
   index=threadinfo->index;

   int lindex;
   lindex = GetLocalIndex(index);

   *(send_buffer[lindex]) = cmd_updateprocarray;
   *(send_buffer[lindex]+1) = index;

   if (send(server_socket[lindex], send_buffer[lindex], 2*sizeof(uint64_t), 0) == -1)
	  printf("update proc array send error\n");
   if (recv(server_socket[lindex], recv_buffer[lindex], sizeof(uint64_t), 0) == -1)
	  printf("update proc array recv error\n");
}

void InitTransactionStructMemAlloc(void)
{
	TransactionData* td;
	THREAD* threadinfo;
	char* memstart;
	Size size;

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	memstart=threadinfo->memstart;

	size=sizeof(TransactionData);

	td=(TransactionData*)MemAlloc((void*)memstart,size);

	if(td==NULL)
	{
		printf("memalloc error.\n");
		return;
	}

	pthread_setspecific(TransactionDataKey,td);

	// to set snapshot-data memory.
	InitTransactionSnapshotDataMemAlloc();

	// to set data memory.
	InitDataMemAlloc();

	// to set data-lock memory.
	InitDataLockMemAlloc();
}

/*
 *start a transaction running environment, reset the
 *transaction's information for a new transaction.
 */
void StartTransaction(void)
{
	InitTransactionSnapshotData();

	// to set data memory.
	InitDataMem();

	// to set data-lock memory.
	InitDataLockMem();

	StartTransactionGetData();
}

void CommitTransaction(void)
{
	TimeStampTz ctime;
	TransactionId tid;
	TransactionData* tdata;

	tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
	tid=tdata->tid;

	ctime = GetTransactionStopTimestamp();

	CommitDataRecord(tid,ctime);

	UpdateProcArray();

	DataLockRelease();

	TransactionMemClean();
}

void AbortTransaction(int trulynum)
{
	TransactionId tid;
	TransactionData* tdata;

	tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
	tid=tdata->tid;

	// process array clean should be after function 'AbortDataRecord'.

	AbortDataRecord(tid, trulynum);

	// once abort, clean the process array after clean updated-data.
	UpdateProcArray();

	DataLockRelease();

	TransactionMemClean();
}

void ReleaseConnect(void)
{
	int i;
	uint64_t release_buffer[1];
	release_buffer[0] = cmd_release_master;
	THREAD* threadinfo;
	int index2;

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index2=threadinfo->index;
	int lindex;
	lindex = GetLocalIndex(index2);

	for (i = 0; i < nodenum; i++)
	{
		if (Send1(lindex, i, cmd_release) == -1)
			printf("release connect server %d send error\n", i);
	}

	if (send(server_socket[lindex], release_buffer, sizeof(release_buffer), 0) == -1)
		printf("release connect master send error\n");

}

void DataReleaseConnect(void)
{
	uint64_t release_buffer[1];

	release_buffer[0] = cmd_release_master;

	if (Send1(0, nodeid, cmd_release) == -1)
		printf("data release connect server send error\n");
	if (send(server_socket[0], release_buffer, sizeof(release_buffer), 0) == -1)
		printf("data release connect master send error\n");
}

void TransactionLoadData(int i)
{
	int j;
	int result;
	int index;
    for (j = 0; j < 20; j++)
	{
      StartTransaction();
      Data_Insert(2, j+1, j+1, nodeid);
      result=PreCommit(&index);
      if(result == 1)
      {
      	CommitTransaction();
      }
      else
      {
      	  //current transaction has to rollback.
      	  AbortTransaction(index);
      }
	}
    DataReleaseConnect();
    ResetProc();
    ResetMem(0);
}

void TransactionRunSchedule(void* args)
{
	//to run transactions according to args.
	int type;
	int rv;
	terminalArgs* param=(terminalArgs*)args;
	type=param->type;

	if(type==0)
	{
		printf("begin LoadData......\n");
		LoadData();
		DataReleaseConnect();
		ResetMem(0);
		ResetProc();
	}
	else
	{
		printf("ready to execute transactions...\n");

		rv=pthread_barrier_wait(&barrier);
		if(rv != 0 && rv != PTHREAD_BARRIER_SERIAL_THREAD)
		{
			printf("Couldn't wait on barrier\n");
			exit(-1);
		}


		printf("begin execute transactions...\n");
		executeTransactions(transactionsPerTerminal, param->whse_id, param->dist_id, param->StateInfo);
		ReleaseConnect();
	}
}

void TransactionContextCommit(TransactionId tid, TimeStampTz ctime, int index)
{
	CommitDataRecord(tid,ctime);

	UpdateProcArray();

	DataLockRelease();
	TransactionMemClean();
}

void TransactionContextAbort(TransactionId tid, int index)
{
	AbortDataRecord(tid, -1);

	// once abort, clean the process array after clean updated-data.
	UpdateProcArray();

	DataLockRelease();
	TransactionMemClean();
}

/*
 *@return:'1' for success, '-1' for rollback.
 *@input:'index':record the truly done data-record's number when PreCommit failed.
 */
int PreCommit(int* index)
{
	char* DataMemStart, *start;
	int num,i,result;
	DataRecord* ptr;


	DataMemStart=(char*)pthread_getspecific(DataMemKey);
	start=DataMemStart+DataNumSize;

	num=*(int*)DataMemStart;

	// sort the data-operation records.
	DataRecordSort((DataRecord*)start, num);

	for(i=0;i<num;i++)
	{
		ptr=(DataRecord*)(start+i*sizeof(DataRecord));

		switch(ptr->type)
		{
		case DataInsert:
			result=TrulyDataInsert(ptr->table_id, ptr->index, ptr->tuple_id, ptr->value, ptr->node_id);
			break;
		case DataUpdate:
			result=TrulyDataUpdate(ptr->table_id, ptr->index, ptr->tuple_id, ptr->value, ptr->node_id);
			break;
		case DataDelete:
			result=TrulyDataDelete(ptr->table_id, ptr->index, ptr->tuple_id, ptr->node_id);
			break;
		default:
			printf("PreCommit:shouldn't arrive here.\n");
		}
		if(result == -1)
		{
			//return to roll back.
			*index=i;
			return -1;
		}
	}
	return 1;
}

int GetNodeId(int index)
{
   return (index/threadnum);
}

int GetLocalIndex(int index)
{
	return (index%threadnum);
}
