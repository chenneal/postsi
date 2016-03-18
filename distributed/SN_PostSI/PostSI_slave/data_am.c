/*
 * data_am.c
 *
 *  Created on: Nov 26, 2015
 *      Author: xiaoxin
 */
/*
 * interface for data access method.
 */
#include<pthread.h>
#include<assert.h>
#include<stdbool.h>
#include<sys/socket.h>
#include"config.h"
#include"data_am.h"
#include"data_record.h"
#include"lock_record.h"
#include"thread_global.h"
#include"proc.h"
#include"trans.h"
#include"trans_conflict.h"
#include"translist.h"
#include"data.h"
#include"socket.h"
#include"transactions.h"
#include"communicate.h"


static bool IsCurrentTransaction(TransactionId tid, TransactionId cur_tid);

int ReadCollusion(int index, int windex, TransactionId tid, TransactionId wtid)
{
   int lindex;

   StartId sid;
   CommitId cid=0;

   int result;

   lindex=GetLocalIndex(index);

   if (TransactionIdIsValid(wtid) && (index != windex) && IsTransactionActive(windex, wtid, false, &sid, &cid))
   {
	   if(cid > 0)
	   {
		   //update local transaction's [s,c].
		   result=ForceUpdateProcSidMax(index, cid);
		   return result;
	   }
	   else
	   {
		   InvisibleTableInsert(windex, lindex, wtid);
	   }
   }
   return 1;
}

void WriteCollusion(TransactionId tid, int index)
{
   int i;
   int rdindex;
   int lindex;

   TransactionId* ReadList=NULL;

   //TransactionId* OldReadList=NULL;

   TransactionId rdtid;

   StartId sid;
   CommitId cid=0;

   lindex=GetLocalIndex(index);

   ReadList=(TransactionId*)pthread_getspecific(NewReadListKey);

   //OldReadList=(TransactionId*)pthread_getspecific(OldReadList);

   //printf("threadnum = %d, nodenum = %d\n", THREADNUM, NODENUM);
   for (i = 0; i < THREADNUM*NODENUM; i++)
   {
	  rdtid = ReadList[i];
	  //rdindex=(rdtid-1)/MaxTransId;
	  if (TransactionIdIsValid(rdtid) && (i != index) && IsTransactionActive(i, rdtid, true, &sid, &cid))
	  {
		  if(cid > 0)
		  {
			  //adjust cid_min of current transaction according to the 'sid'.
			  //UpdateProcCommitId(i,sid);
			  ForceUpdateProcCidMin(index, sid);
		  }
		  else
		  {
			  InvisibleTableInsert(lindex, i, rdtid);
		  }
	  }
   }
}

/*
 * @return: '0' to rollback, '1' to go head.
 */
int Data_Insert(int table_id, TupleId tuple_id, TupleId value, int nid)
{
	//printf("enter insert\n");
	int index=0;
	int status;
	int h;
	DataRecord datard;
	TransactionData* tdata;
	TransactionId tid;
	THREAD* threadinfo;

	//get the pointer to current transaction data.
	tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
	tid=tdata->tid;

	//get the pointer to current thread information.
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index=threadinfo->index;
	/*
	 * the node transaction process must to get the data from the storage process in the
	 * node itself or in the other node, both use the socket to communicate.
	 */
	int lindex;
	lindex = GetLocalIndex(index);

    if ((Send5(lindex, nid, cmd_insert, table_id, tuple_id, tid, index)) == -1)
       printf("insert send error!\n");
    if ((Recv(lindex, nid, 2)) == -1)
       printf("insert recv error!\n");

    status = *(recv_buffer[lindex]);
    h = *(recv_buffer[lindex]+1);
    //printf("status = %d, h = %d\n", status, h);
    if (status == 0)
    	return 0;

	//WriteCollusion(table_id, h, tid, index);
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
 * @return:'0' for not found, '1' for success.
 */
int Data_Update(int table_id, TupleId tuple_id, TupleId value, int nid)
{
	int index=0;
	int wr_index;

	int h;
	int status;

	CommitId cid=0;
	StartId sid;

	DataRecord datard;

	TransactionData* tdata;
	TransactionId tid, wr_tid;
	THREAD* threadinfo;

	int flag;
	int result;

	//get the pointer to current transaction data.
	tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
	tid=tdata->tid;

	//get the pointer to current thread information.
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index=threadinfo->index;

	int lindex;
	lindex = GetLocalIndex(index);

	//if (Send3(lindex, nid, cmd_updatefind, table_id, tuple_id) == -1)
	if(Send5(lindex, nid, cmd_updatefind, table_id, tuple_id, tid, index) == -1)
		printf("update find send error\n");
	//if (Recv(lindex, nid, 2) == -1)
	if (Recv(lindex, nid, 3) == -1)
		printf("update find recv error\n");

	//printf("after update find\n");
	status = *(recv_buffer[lindex]);
	h  = *(recv_buffer[lindex] + 1);

	wr_tid=(TransactionId)(*(recv_buffer[lindex] + 2));

    if (status == 0)
    	return 0;

    /*
     * xxzhou: the data by 'tuple_id' exists, deal with the wr_tid.
     */
    wr_index=(wr_tid-1)/MaxTransId;

    result=ReadCollusion(index, wr_index, tid, wr_tid);

    if(result==0)
    {
    	//return to abort
    	return -1;
    }

    /*
     * xxzhou: light-read before update here.
     */
	//Data_Read(table_id, tuple_id, nid);

	/*
	//by here, we are sure that we can update the data.
	if (Send5(lindex, nid, cmd_updatewritelistinsert, table_id, h, tid, index) == -1)
		printf("update write list insert send error\n");
	if (recv(connect_socket[nid][lindex], recv_buffer[lindex], READLISTMAX * sizeof(uint64_t), 0) == -1)
		printf("update writ list insert recv error\n");
	//to modify the interface.
	WriteCollusion(index, tid, recv_buffer[lindex]);
	*/
	//interface:update(table_id,index);

	//record the updated data.
	//get the data pointer.

	datard.type=DataUpdate;

	datard.table_id=table_id;
	datard.tuple_id=tuple_id;

	datard.value=value;

	datard.index=h;
	datard.node_id = nid;
	DataRecordInsert(&datard);
	return 1;
}

/*
 * @return:'0' for not found, '1' for success.
 */
int Data_Delete(int table_id, TupleId tuple_id, int nid)
{
	int index=0;
	int wr_index;

	int h;
	int status;

	CommitId cid=0;
	StartId sid;

	DataRecord datard;

	TransactionData* tdata;
	TransactionId tid, wr_tid;
	THREAD* threadinfo;

	int flag;
	int result;

	//get the pointer to current transaction data.
	tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
	tid=tdata->tid;

	//get the pointer to current thread information.
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index=threadinfo->index;
	int lindex;
	lindex = GetLocalIndex(index);
	//get the index of 'tuple_id' in table 'table_id'.
	//index=hashsearch(table_id,tuple_id);
	//if (Send3(lindex, nid, cmd_updatefind, table_id, tuple_id) == -1)
	if (Send5(lindex, nid, cmd_updatefind, table_id, tuple_id, tid, index) == -1)
		printf("update find send error\n");
	if (Recv(lindex, nid, 3) == -1)
		printf("update find recv error\n");
	//printf("after update find\n");
	status = *(recv_buffer[lindex]);
	h  = *(recv_buffer[lindex] + 1);

	wr_tid=(TransactionId)(*(recv_buffer[lindex] + 2));

    if (status == 0)
    	return 0;

    /*
     * xxzhou: the data by 'tuple_id' exists, deal with the wr_tid.
     */
    wr_index=(wr_tid-1)/MaxTransId;

    result=ReadCollusion(index, wr_index, tid, wr_tid);
    if(result==0)
    {
    	//return to abort.
    	return -1;
    }

	//Data_Read(table_id, tuple_id, nid);

	/*
	//by here, we are sure that we can update the data.
	if (Send5(lindex, nid, cmd_updatewritelistinsert, table_id, h, tid, index) == -1)
		printf("update write list insert send error\n");
	if (recv(connect_socket[nid][lindex], recv_buffer[lindex], READLISTMAX * sizeof(uint64_t), 0) == -1)
		printf("update writ list insert recv error\n");
	//to modify the interface.
	WriteCollusion(index, tid, recv_buffer[lindex]);
	*/
	//interface:update(table_id,index);

	//record the updated data.
	//get the data pointer.

	datard.type=DataDelete;

	datard.table_id=table_id;
	datard.tuple_id=tuple_id;

	datard.index=h;
	datard.node_id = nid;
	DataRecordInsert(&datard);
	return 1;
}

/*
 * @input:'isupdate':true for reading before updating, false for commonly reading.
 * @return:0 for read nothing, to rollback or just let it go, else return 'value'.
 */
TupleId Data_Read(int table_id, TupleId tuple_id, int nid, int* flag)
{
	//printf("data_read: enter\n");
	StartId sid_max;
	StartId sid_min;
	CommitId cid_min;

	CommitId cid;
	int h;
	int index;
	int status;
	int windex;
	uint64_t value;
	TransactionId wtid;
	TupleId visible;
	char* DataMemStart=NULL;

	int result;

	TransactionData* tdata;
	TransactionId tid;
	THREAD* threadinfo;
	//get the pointer to current thread information.
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index=threadinfo->index;
	int lindex;
	lindex = GetLocalIndex(index);

	uint64_t* buffer;
	//get the pointer to current transaction data.
	tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
	tid=tdata->tid;

	*flag=1;

	//maybe the 'sid_max' can be passed as a parameter.
	//sid_max=GetTransactionSidMax(threadinfo->index);
	//printf("tid:%d\n",tid);
	DataMemStart=(char*)pthread_getspecific(DataMemKey);

	// find a place to read, insert the read list transaction, and return the write list transaction id.
	if(Send5(lindex, nid, cmd_readfind, table_id, tuple_id, tid, index) == -1)
	   printf("read find send error\n");
	if (Recv(lindex, nid, 4) == -1)
	   printf("read find lock send error\n");
	status = *(recv_buffer[lindex]);
	wtid = *(recv_buffer[lindex]+1);
	windex = *(recv_buffer[lindex]+2);
	h = *(recv_buffer[lindex]+3);

	//roll back
	if (status == 0)
	{
		*flag=0;
		return 0;
	}

	result=ReadCollusion(index, windex, tid, wtid);
	if(result==0)
	{
		*flag=-3;
		return 0;
	}
/*
	newest = (HashTable[h].rear + VERSIONMAX -1) % VERSIONMAX;
	//the data by (table_id,tuple_id) is being updated.
	if(newest > HashTable[h].lcommit)
	{
		//to do nothing here.
	}
*/
	visible=IsDataRecordVisible(DataMemStart, table_id, tuple_id, nid);
	if(visible == -1)
	{
		//current transaction has deleted the tuple to read, so return to rollback.
		//pthread_spin_unlock(&RecordLatch[table_id][h]);
		*flag=-1;
		return 0;
	}
	else if(visible > 0)
	{
		//see own transaction's update.
		//pthread_spin_unlock(&RecordLatch[table_id][h]);
		//return (void*)&HashTable[h].VersionList[0];
		//return HashTable[h].tupleid;
		return visible;
	}


	sid_max=GetTransactionSidMax(lindex);
	sid_min=GetTransactionSidMin(lindex);
	cid_min=GetTransactionCidMin(lindex);

	if (Send6(lindex, nid, cmd_readversion, table_id, h, sid_min, sid_max, cid_min) == -1)
		printf("read version send error\n");
    if (Recv(lindex, nid, 4) == -1)
    	printf("read version recv error\n");

    //buffer=(uint64_t*)recv_buffer[lindex];

        status=*(recv_buffer[lindex]);
    sid_min=*(recv_buffer[lindex]+1);
    cid_min=*(recv_buffer[lindex]+2);
    value=*(recv_buffer[lindex]+3);
    //printf("status = %d, s_min = %d, c_min = %d, value = %d, tupleid = %ld, tableid = %d\n", status, sid_min, cid_min, value, tuple_id, table_id);

    if (status == 0)
    {
    	//read nothing.
    	*flag=-4;
    	return 0;
    }
    else if(status == 1)
    {
    	//read a deleted version.
    	*flag=-2;
    	return 0;
    }

    result=MVCCUpdateProcId(lindex, sid_min, cid_min);
    if(result==0)
    {
        *flag = -5;
    	//return to abort current transaction.
    	return 0;
    }

    return value;
}
/*
TupleId LocateData_Read(int table_id, int h, TupleId *id)
{
	VersionId i;
	int index, c_id;
	TupleId visible, tuple_id;

	VersionId newread,newest;
	//Version* v;
	char* DataMemStart=NULL;

	TransactionData* tdata;
	TransactionId tid;
	THREAD* threadinfo;
	//get the pointer to current transaction data.
	tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
	tid=tdata->tid;

	//get the pointer to current thread information.
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index=threadinfo->index;

	DataMemStart=(char*)pthread_getspecific(DataMemKey);

	THash HashTable = TableList[table_id];

	if(HashTable[h].tupleid == InvalidTupleId)
	{
		return 0;
	}

	tuple_id=HashTable[h].tupleid;
	//we should add to the read list before reading.
	//ReadListInsert(table_id, h, tid);
	//ReadCollusion(table_id, h, tid,index);

	pthread_spin_lock(&RecordLatch[table_id][h]);
	newest = (HashTable[h].rear + VERSIONMAX -1) % VERSIONMAX;
	//the data by (table_id,tuple_id) is being updated.
	if(newest > HashTable[h].lcommit)
	{
		//to do nothing here.
	}

	visible=IsDataRecordVisible(DataMemStart, table_id, tuple_id);
	if(visible == -1)
	{
		//current transaction has deleted the tuple to read, so return to rollback.
		pthread_spin_unlock(&RecordLatch[table_id][h]);
		return 0;
	}
	else if(visible > 0)
	{
		//see own transaction's update.
		pthread_spin_unlock(&RecordLatch[table_id][h]);
		//return (void*)&HashTable[h].VersionList[0];
		//return HashTable[h].tupleid;
		*id=tuple_id;
		return visible;
	}

	//by here, we try to read already committed tuple.
	if(HashTable[h].lcommit >= 0)
	{
		for (i = HashTable[h].lcommit; i != (HashTable[h].front + VERSIONMAX - 1) % VERSIONMAX; i = (i-1) % VERSIONMAX)
		{
			//
			if (MVCCVisible(&(HashTable[h]), i) )
			{
				if(IsMVCCDeleted(&HashTable[h],i))
				{
					pthread_spin_unlock(&RecordLatch[table_id][h]);
					return 0;
				}
				else
				{
					pthread_spin_unlock(&RecordLatch[table_id][h]);
					//return (void*)&HashTable[h].VersionList[i];
					//return HashTable[h].tupleid;
					*id=tuple_id;
					return HashTable[h].VersionList[i].value;
				}
			}
		}
	}
	pthread_spin_unlock(&RecordLatch[table_id][h]);

	return 0;


}
*/

/*
 * used for read operation during update and delete operation.
 * no need to access the data of the tuple.
 * @return: '0' to abort, '1' to go head.
 */
int Light_Data_Read(int table_id, int h)
{
	int index, c_id;

	int result;

	TransactionData* tdata;
	TransactionId tid;
	THREAD* threadinfo;

	//get the pointer to current transaction data.
	tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
	tid=tdata->tid;

	//get the pointer to current thread information.
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index=threadinfo->index;

	THash HashTable = TableList[table_id];

	//we should add to the read list before reading.
	ReadListInsert(table_id, h, tid, index);

	result=ReadCollusion(table_id, h, tid,index);

	return result;
}

bool IsCurrentTransaction(TransactionId tid, TransactionId cur_tid)
{
	return tid == cur_tid;
}

/*
 * @return:'1' for success, '-1' for rollback.
 */
int TrulyDataInsert(int table_id, uint64_t index, TupleId tuple_id, TupleId value, int nid)
{
	int status;
	int index2;
	THREAD* threadinfo;
	//int index,tableid;
	TransactionData* tdata;
	TransactionId tid;
	DataLock lockrd;
	//get the pointer to current thread information.
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index2=threadinfo->index;
	int lindex;
	lindex = GetLocalIndex(index2);
	tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
	tid=tdata->tid;

    //if((Send5(lindex, nid, cmd_trulyinsert, table_id, value, index, tid)) == -1)
	if((Send6(lindex, nid, cmd_trulyinsert, table_id, tuple_id, value, index, tid)) == -1)
    	printf("truly insert send error!\n");
    if((Recv(lindex, nid, 1)) == -1)
    	printf("truly insert recv error!\n");

    status = *(recv_buffer[lindex]);
    if (status == 4)
    	return -1;

	//record the lock.
	lockrd.table_id=table_id;
	lockrd.tuple_id=tuple_id;
	lockrd.lockmode=LOCK_EXCLUSIVE;
	lockrd.index=index;
	lockrd.node_id = nid;
	DataLockInsert(&lockrd);
	return 1;
}

/*
 * @return:'1' for success, '-1' for rollback.
 */
int TrulyDataUpdate(int table_id, uint64_t index, TupleId tuple_id, TupleId value, int nid)
{
	//int index,tableid;
	int index2;
	StartId sid_max;
	StartId sid_min;
	CommitId cid, cid_min;
	int status;
	int result;
	THREAD* threadinfo;
	bool firstadd=false;
	bool isdelete=false;
	TransactionData* tdata;
	TransactionId tid;
	DataLock lockrd;

	tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
	tid=tdata->tid;
	//get the pointer to current thread information.
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index2=threadinfo->index;
	int lindex;
	lindex = GetLocalIndex(index2);

	sid_max=GetTransactionSidMax(lindex);
	sid_min=GetTransactionSidMin(lindex);
	cid_min=GetTransactionCidMin(lindex);

	//printf("TrulyDataUpdate: %d %d .\n",tableid,index);
	//to void repeatedly add lock.
	if(IsWrLockHolding(table_id,tuple_id,nid) == 0)
	{
		firstadd=true;
	}
	//printf("TrulyDataUpdate: after holdlock.\n");
	if (Send8(lindex, nid, cmd_updateconflict, table_id, index, tid, firstadd, sid_max, sid_min, cid_min) == -1)
		printf("update conflict send error\n");
	if (Recv(lindex, nid, READLISTMAX+3) == -1)
		printf("update conflict recv error\n");

	status = *(recv_buffer[lindex]);
	sid_min = *(recv_buffer[lindex]+1);
	cid_min= *(recv_buffer[lindex]+2);
	if (status == 4)
		return -1;

	//record the lock.
	lockrd.table_id=table_id;
	lockrd.tuple_id=tuple_id;
	lockrd.index = index;
	lockrd.lockmode=LOCK_EXCLUSIVE;
	lockrd.node_id = nid;
	DataLockInsert(&lockrd);

	//add process elem lock here.
	result=MVCCUpdateProcId(lindex, sid_min, cid_min);

	if(result==0)
		return -1;

	//merge-read-list
	MergeReadList(recv_buffer[lindex]+3);
	//by here, we have hold the write-lock.

	//printf("TrulyDataUpdate: after IsUpdateConflict.\n");\

	//printf("TrulyDataUpdate: after lock record table_id:%d,index:%d.\n",table_id, index);

	//here are the place we truly update the data.
	if (Send6(lindex, nid, cmd_updateversion, table_id, index, tid, value, isdelete) == -1)
		printf("update version send error\n");
	if (Recv(lindex, nid, 1) == -1)
		printf("update version recv error\n");
	return 1;
}

/*
 * @return:'1' for success, '-1' for rollback.
 */
int TrulyDataDelete(int table_id, uint64_t index, TupleId tuple_id, int nid)
{
	//int index,tableid;
	int index2;
	StartId sid_max;
	StartId sid_min;
	CommitId cid;
	CommitId cid_min;
	int status;
	int result;

	THREAD* threadinfo;
	bool firstadd=false;
	bool isdelete=true;
	uint64_t value = 0;
	TransactionData* tdata;
	TransactionId tid;
	DataLock lockrd;

	tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
	tid=tdata->tid;
	//get the pointer to current thread information.
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index2=threadinfo->index;
	int lindex;
	lindex = GetLocalIndex(index2);

	sid_max=GetTransactionSidMax(lindex);
	sid_min=GetTransactionSidMin(lindex);
	cid_min=GetTransactionCidMin(lindex);

	//GetPosition((Record*)data,&tableid,&index);
	//tableid=table_id;

	//printf("TrulyDataUpdate: %d %d .\n",tableid,index);
	//to void repeatedly add lock.
	if(IsWrLockHolding(table_id,tuple_id,nid) == 0)
	{
		firstadd=true;
	}
	//printf("TrulyDataUpdate: after holdlock.\n");
	if (Send8(lindex, nid, cmd_updateconflict, table_id, index, tid, firstadd, sid_max, sid_min, cid_min) == -1)
		printf("update conflict send error\n");
	if (Recv(lindex, nid, READLISTMAX+3) == -1)
		printf("update conflict recv error\n");
	status = *(recv_buffer[lindex]);
	sid_min = *(recv_buffer[lindex]+1);
	cid_min = *(recv_buffer[lindex]+2);

	if (status == 4)
		return -1;

	//record the lock.
	lockrd.table_id=table_id;
	lockrd.tuple_id=tuple_id;
	lockrd.index = index;
	lockrd.lockmode=LOCK_EXCLUSIVE;
	lockrd.node_id = nid;
	DataLockInsert(&lockrd);

	result=MVCCUpdateProcId(lindex, sid_min, cid_min);

	if(result==0)
		return -1;

	//merge-read-list
	MergeReadList(recv_buffer[lindex]+3);

	//printf("TrulyDataUpdate: after lock record table_id:%d,index:%d.\n",table_id, index);

	//here are the place we truly update the data.
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
			fprintf(fp,"(%2d %ld %ld %2d)",rd->VersionList[k].tid,rd->VersionList[k].cid,rd->VersionList[k].value,rd->VersionList[k].deleted);
		fprintf(fp,"\n");
	}
	printf("\n");
}
