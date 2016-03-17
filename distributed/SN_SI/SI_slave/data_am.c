/*
 * data_am.c
 *
 *  Created on: Dec 7, 2015
 *      Author: xiaoxin
 */

/*
 * interface for data access method.
 */

#include<pthread.h>
#include<assert.h>
#include<stdbool.h>
#include<sys/socket.h>
#include<assert.h>
#include"config.h"
#include"data_am.h"
#include"data_record.h"
#include"lock_record.h"
#include"thread_global.h"
#include"proc.h"
#include"trans.h"
#include"snapshot.h"
#include"transactions.h"
#include"data.h"
#include"socket.h"
#include"communicate.h"

static bool IsCurrentTransaction(TransactionId tid, TransactionId cur_tid);

/*
 * @return: '0' to rollback, '1' to go head.
 */
int Data_Insert(int table_id, TupleId tuple_id, TupleId value, int nid)
{
	int index;
	int status;
	uint64_t h;
	DataRecord datard;
	THREAD* threadinfo;

	/* get the pointer to current thread information. */
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index=threadinfo->index;

	/*
	 * the node transaction process must to get the data from the storage process in the
	 * node itself or in the other nodes, both use the socket to communicate.
	 */

	int lindex;
	lindex = GetLocalIndex(index);
    if ((Send3(lindex, nid, cmd_insert, table_id, tuple_id)) == -1)
       printf("insert send error!\n");
    if ((Recv(lindex, nid, 2)) == -1)
       printf("insert recv error!\n");

    status = *(recv_buffer[lindex]);
    h = *(recv_buffer[lindex]+1);

    if (status == 0)
    	return 0;

	datard.type=DataInsert;
	datard.table_id=table_id;
	datard.tuple_id=tuple_id;
	datard.value=value;
	datard.index=h;
	datard.node_id = nid;
	DataRecordInsert(&datard);

	return 1;
}

/*
 * @return:'0' for not found, '1' for success, '-1' for update-conflict-rollback.
 */
int Data_Update(int table_id, TupleId tuple_id, TupleId value, int nid)
{
	int index=0;
	int h;
	DataRecord datard;

    int status;
	THREAD* threadinfo;

	/* get the pointer to current thread information. */
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index=threadinfo->index;
	int lindex;
	lindex = GetLocalIndex(index);
	if (Send3(lindex, nid, cmd_updatefind, table_id, tuple_id) == -1)
		printf("update find send error\n");
	if (Recv(lindex, nid, 2) == -1)
		printf("update find recv error\n");

	status = *(recv_buffer[lindex]);
	h  = *(recv_buffer[lindex]+1);
    if (status == 0)
    	return 0;

	/* record the updated data. */
	datard.type=DataUpdate;
	datard.table_id=table_id;
	datard.tuple_id=tuple_id;
	datard.value=value;
    datard.node_id = nid;
	datard.index=h;
	DataRecordInsert(&datard);
	return 1;
}

/*
 * @return:'0' for not found, '1' for success, '-1' for update-conflict-rollback.
 */

int Data_Delete(int table_id, TupleId tuple_id, int nid)
{
	int index=0;
	int h;
	DataRecord datard;
    int status;
	THREAD* threadinfo;
	/* get the pointer to current thread information. */
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index=threadinfo->index;
	int lindex;
	lindex = GetLocalIndex(index);

	if (Send3(lindex, nid, cmd_updatefind, table_id, tuple_id) == -1)
		printf("delete find send error\n");
	if (Recv(lindex, nid, 2) == -1)
		printf("delete find recv error\n");

	status = *(recv_buffer[lindex]);
	h  = *(recv_buffer[lindex]+1);
    if (status == 0)
    	return 0;

	/* record the updated data. */
	datard.type=DataDelete;
	datard.table_id=table_id;
	datard.tuple_id=tuple_id;
    datard.node_id = nid;
	datard.index = h;
	DataRecordInsert(&datard);
	return 1;
}

/*
 * @input:'isupdate':true for reading before updating, false for commonly reading.
 * @return:NULL for read nothing, to rollback or just let it go.
 */
TupleId Data_Read(int table_id, TupleId tuple_id, int nid, int* flag)
{
	int i;
	int h;
	int index, visible;
	int status;
	uint64_t value;
	Snapshot* snap;
	char* DataMemStart;

    int size;
	*flag=1;

    /* send size */
	size = 3 + 3 + MAXPROCS;

	THREAD* threadinfo;
	/* get the pointer to current thread information. */
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index=threadinfo->index;
	int lindex;
	lindex = GetLocalIndex(index);
	/* get the pointer to data-memory. */
	DataMemStart=(char*)pthread_getspecific(DataMemKey);

	/* get pointer to transaction-snapshot-data. */
	snap=(Snapshot*)pthread_getspecific(SnapshotDataKey);

	if (Send3(lindex, nid, cmd_readfind, table_id, tuple_id) == -1)
		printf("read find send error\n");
	if (Recv(lindex, nid, 2) == -1)
		printf("read find recv error\n");

	status = *(recv_buffer[lindex]);
	h  = *(recv_buffer[lindex]+1);

    if (status == 0)
    {
    	*flag = 0;
    	return 0;
    }

	visible=IsDataRecordVisible(DataMemStart, table_id, tuple_id, nid);
	if(visible == -1)
	{
		/* current transaction has deleted the tuple to read, so return to rollback. */
		*flag=-1;
		return 0;
	}
	else if(visible > 0)
	{
		/* see own transaction's update. */
		return visible;
	}

	*(send_buffer[lindex]) = cmd_readversion;
	*(send_buffer[lindex]+1) = snap->tcount;
	*(send_buffer[lindex]+2) = snap->tid_min;
	*(send_buffer[lindex]+3) = snap->tid_max;
	for (i = 0; i < MAXPROCS; i++)
		*(send_buffer[lindex]+4+i) = snap->tid_array[i];
	*(send_buffer[lindex]+4+i) = table_id;
	*(send_buffer[lindex]+5+i) = h;

	if (send(connect_socket[nid][lindex], send_buffer[lindex], size*sizeof(uint64_t), 0) == -1)
		printf("read version send error\n");
    if (Recv(lindex, nid, 2) == -1)
    	printf("read version recv error\n");

    status = *(recv_buffer[lindex]);
    value = *(recv_buffer[lindex]+1);

    if (status == 4)
    {
    	*flag = -2;
    	return 0;
    }

    if (status == 0)
    {
    	*flag = -3;
    	return 0;
    }

    if (status == 1)
    	return value;

	return 0;
}

bool IsCurrentTransaction(TransactionId tid, TransactionId cur_tid)
{
	return tid == cur_tid;
}

/*
 * @return:'1' for success, '-1' for rollback.
 */
int TrulyDataInsert(int table_id, int index, TupleId tuple_id, TupleId value, int nid)
{
	int index2;
	int status;
	TransactionData* tdata;
	TransactionId tid;
	DataLock lockrd;
	THREAD* threadinfo;

	tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
	tid=tdata->tid;

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index2=threadinfo->index;

	int lindex;
	lindex = GetLocalIndex(index2);

    if((Send6(lindex, nid, cmd_trulyinsert, table_id, tuple_id, value, index, tid)) == -1)
    	printf("truly insert send error!\n");
    if((Recv(lindex, nid, 1)) == -1)
    	printf("truly insert recv error!\n");

	/* record the lock. */
	lockrd.table_id=table_id;
	lockrd.tuple_id=tuple_id;
	lockrd.index = index;
	lockrd.node_id = nid;
	lockrd.lockmode=LOCK_EXCLUSIVE;
	DataLockInsert(&lockrd);

    status = *(recv_buffer[lindex]);
    if (status == 4)
    	return -1;
	return 1;
}

/*
 * @return:'1' for success, '-1' for rollback.
 */
int TrulyDataUpdate(int table_id, int index, TupleId tuple_id, TupleId value, int nid)
{
	int i;
	int index2;
	bool firstadd=false;
	bool isdelete = false;
	TransactionData* tdata;
	TransactionId tid;
	DataLock lockrd;
	Snapshot* snap;
	int status = 0;
	THREAD* threadinfo;

	int size = MAXPROCS + 1 + 3 + 4;

	/* get the pointer to current thread information. */
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index2=threadinfo->index;
	int lindex;
	lindex = GetLocalIndex(index2);

	tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
	tid=tdata->tid;

	/* get the pointer to transaction-snapshot-data. */
	snap=(Snapshot*)pthread_getspecific(SnapshotDataKey);

	/* to void repeatedly add lock. */
	if(!IsWrLockHolding(table_id,tuple_id,nid))
	{
		/* the first time to hold the wr-lock on data (table_id,tuple_id). */
		firstadd=true;
	}

	*(send_buffer[lindex]) = cmd_updateconflict;
	*(send_buffer[lindex]+1) = snap->tcount;
	*(send_buffer[lindex]+2) = snap->tid_min;
	*(send_buffer[lindex]+3) = snap->tid_max;
    for (i = 0; i < MAXPROCS; i++)
    	*(send_buffer[lindex]+4+i) = snap->tid_array[i];
    *(send_buffer[lindex]+4+i) = table_id;
    *(send_buffer[lindex]+5+i) = index;
    *(send_buffer[lindex]+6+i) = tid;
    *(send_buffer[lindex]+7+i) = firstadd;

    if (send(connect_socket[nid][lindex], send_buffer[lindex], size*sizeof(uint64_t), 0) == -1)
    	printf("update conflict send error\n");
	if (Recv(lindex, nid, 1) == -1)
		printf("update conflict recv error\n");

	status = *(recv_buffer[lindex]);
	if (status == 4)
		return -1;

	/* record the lock. */
	lockrd.table_id=table_id;
	lockrd.tuple_id=tuple_id;
	lockrd.index = index;
	lockrd.node_id = nid;
	lockrd.lockmode=LOCK_EXCLUSIVE;
	DataLockInsert(&lockrd);

	if (Send6(lindex, nid, cmd_updateversion, table_id, index, tid, value, isdelete) == -1)
		printf("update version send error\n");
	if (Recv(lindex, nid, 1) == -1)
		printf("update version recv error\n");
	return 1;
}

/*
 * @return:'1' for success, '-1' for rollback.
 */
int TrulyDataDelete(int table_id, int index, TupleId tuple_id, int nid)
{
	int i;
	int index2;
	bool firstadd=false;
	bool isdelete = true;
	uint64_t value = 0;
	TransactionData* tdata;
	TransactionId tid;
	DataLock lockrd;
	Snapshot* snap;
	int status = 0;
	THREAD* threadinfo;

	int size = MAXPROCS + 1+ 3 + 4;

	/* get the pointer to current thread information. */
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index2=threadinfo->index;
	int lindex;
	lindex = GetLocalIndex(index2);
	tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
	tid=tdata->tid;

	/* get the pointer to transaction-snapshot-data. */
	snap=(Snapshot*)pthread_getspecific(SnapshotDataKey);

	/* to void repeatedly add lock. */
	if(!IsWrLockHolding(table_id,tuple_id,nid))
	{
		/* the first time to hold the wr-lock on data (table_id,tuple_id). */
		firstadd=true;
	}

	*(send_buffer[lindex]) = cmd_updateconflict;
	*(send_buffer[lindex]+1) = snap->tcount;
	*(send_buffer[lindex]+2) = snap->tid_min;
	*(send_buffer[lindex]+3) = snap->tid_max;
    for (i = 0; i < MAXPROCS; i++)
    	*(send_buffer[lindex]+4+i) = snap->tid_array[i];
    *(send_buffer[lindex]+4+i) = table_id;
    *(send_buffer[lindex]+5+i) = index;
    *(send_buffer[lindex]+6+i) = tid;
    *(send_buffer[lindex]+7+i) = firstadd;

    if (send(connect_socket[nid][lindex], send_buffer[lindex], size*sizeof(uint64_t), 0) == -1)
    	printf("update conflict send error\n");
	if (Recv(lindex, nid, 1) == -1)
		printf("update conflict recv error\n");

	status = *(recv_buffer[lindex]);
	if (status == 4)
		return -1;

	/* record the lock. */
	lockrd.table_id=table_id;
	lockrd.tuple_id=tuple_id;
	lockrd.index = index;
	lockrd.node_id = nid;
	lockrd.lockmode=LOCK_EXCLUSIVE;
	DataLockInsert(&lockrd);

	if (Send6(lindex, nid, cmd_updateversion, table_id, index, tid, value, isdelete) == -1)
		printf("update version send error\n");
	if (Recv(lindex, nid, 1) == -1)
		printf("update version recv error\n");
	return 1;
}

void PrintTable(int table_id)
{
	int i,j,k;
	THash HashTable;
	Record* rd;
	char filename[10];

	FILE* fp;

	memset(filename,'\0',sizeof(filename));

	filename[0]=(char)(table_id+'0');
        filename[1]=(char)('+');
        filename[2]=(char)(nodeid+'0');
	strcat(filename, ".txt");

	if((fp=fopen(filename,"w"))==NULL)
	{
		printf("file open error\n");
		exit(-1);
	}

	HashTable=TableList[i];
	for(j=0;j<RecordNum[i];j++)
	{
		rd=&HashTable[j];
		fprintf(fp,"%d: %ld",j,rd->tupleid);
		for(k=0;k<VERSIONMAX;k++)
			fprintf(fp,"(%2d %ld %ld %2d)",rd->VersionList[k].tid,rd->VersionList[k].committime,rd->VersionList[k].value,rd->VersionList[k].deleted);
		fprintf(fp,"\n");
	}
	printf("\n");
}
