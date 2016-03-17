/*
 * data_am.c
 *
 *  Created on: Nov 26, 2015
 *      Author: xiaoxin
 */
/*
 * interface for data access method.
 */
#include <pthread.h>
#include <assert.h>
#include <stdbool.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#include "config.h"
#include "data_am.h"
#include "data_record.h"
#include "lock_record.h"
#include "thread_global.h"
#include "proc.h"
#include "trans.h"
#include "trans_conflict.h"
#include "translist.h"
#include "transactions.h"
#include "lock.h"

/* initialize the record hash table and the record lock table, latch table. by yu*/
pthread_rwlock_t* RecordLock[TABLENUM];
pthread_spinlock_t* RecordLatch[TABLENUM];
Record* TableList[TABLENUM];

inline bool ReadMVCCVisible(Record * r, VersionId v, StartId* sid_min, CommitId* cid_min);

inline bool MVCCVisible(Record * r, VersionId v);

bool IsInsertDone(int table_id, int index);

static void PrimeBucketSize(void);

static void ReadPrimeTable(void);

int BucketNum[TABLENUM];
int BucketSize[TABLENUM];
uint64_t RecordNum[TABLENUM];

int Prime[150000];
int PrimeNum;

/*
 * we initialize a ring-queue here to manage the multi-versions of one tuple,
 */
void InitQueue(Record * r)
{
   int i;
   assert(r != NULL);
   r->tupleid = InvalidTupleId;
   r->rear = 0;
   r->front = 0;
   // lcommit is means the last version id that commit, its initialized id should be -1 to represent the nothing position
   r->lcommit = -1;
   for (i = 0; i < VERSIONMAX; i++)
   {
      r->VersionList[i].tid = 0;
      r->VersionList[i].cid = 0;
      r->VersionList[i].deleted = false;

      r->VersionList[i].value=0;
   }
}

bool isFullQueue(Record * r)
{
   if ((r->rear + 1) % VERSIONMAX == r->front)
      return true;
   else
      return false;
}

bool isEmptyQueue(Record * r)
{
	if(r->lcommit == -1)
		return true;
	else
		return false;
}

/*
 * append a new version to the ring-queue of one tuple.
 */
void EnQueue(Record * r, TransactionId tid, TupleId value)
{
   if(isFullQueue(r))
   {
	   printf("EnQueue failed, %d %d %d\n",r->front,r->rear,r->lcommit);
	   exit(-1);
   }
   r->VersionList[r->rear].tid = tid;
   r->VersionList[r->rear].value= value;

   r->rear = (r->rear + 1) % VERSIONMAX;
}

void InitBucketNum_Size(void)
{
	//bucket num.
	BucketNum[Warehouse_ID]=1;
    BucketNum[Item_ID]=1;
    BucketNum[Stock_ID]=configWhseCount;
    BucketNum[District_ID]=configWhseCount;
    BucketNum[Customer_ID]=configWhseCount*configDistPerWhse;
    BucketNum[History_ID]=configWhseCount*configDistPerWhse;
    BucketNum[Order_ID]=configWhseCount*configDistPerWhse;
    BucketNum[NewOrder_ID]=configWhseCount*configDistPerWhse;
    BucketNum[OrderLine_ID]=configWhseCount*configDistPerWhse;

    //bucket size.
	BucketSize[Warehouse_ID]=configWhseCount;
	BucketSize[Item_ID]=configUniqueItems;
	BucketSize[Stock_ID]=configUniqueItems;
	BucketSize[District_ID]=configDistPerWhse;
	BucketSize[Customer_ID]=configCustPerDist;
	BucketSize[History_ID]=configCustPerDist;
	BucketSize[Order_ID]=OrderMaxNum;
	BucketSize[NewOrder_ID]=OrderMaxNum;
	BucketSize[OrderLine_ID]=OrderMaxNum*10;

	//adapt the bucket-size to prime.
	ReadPrimeTable();
	PrimeBucketSize();
}

void InitRecordNum(void)
{
	int i;

	for(i=0;i<TABLENUM;i++)
		RecordNum[i]=BucketNum[i]*BucketSize[i];
}

/*
 * memory allocation for each table.
 */
void InitRecordMem(void)
{
	int i;

	for(i=0;i<TABLENUM;i++)
	{
		TableList[i]=(Record*)malloc(sizeof(Record)*RecordNum[i]);
		if(TableList[i]==NULL)
		{
			printf("record memory allocation failed for table %d.\n",i);
			exit(-1);
		}
	}
}

void InitLatchMem(void)
{
	int i;

	for(i=0;i<TABLENUM;i++)
	{
		RecordLock[i]=(pthread_rwlock_t*)malloc(sizeof(pthread_rwlock_t)*RecordNum[i]);
		RecordLatch[i]=(pthread_spinlock_t*)malloc(sizeof(pthread_spinlock_t)*RecordNum[i]);
		if(RecordLock[i]==NULL || RecordLatch[i]==NULL)
		{
			printf("memory allocation failed for record-latch %d.\n",i);
			exit(-1);
		}
	}
}

/*
 * initialize the hash table structure and the related lock for each table.
 */
void InitRecord(void)
{
   InitBucketNum_Size();

   InitRecordNum();

   InitRecordMem();

   InitLatchMem();

   int i;
   uint64_t j;
   for (i = 0; i < TABLENUM; i++)
   {
      for (j = 0; j < RecordNum[i]; j++)
      {
         InitQueue(&TableList[i][j]);
      }
   }
   for (i = 0; i < TABLENUM; i++)
   {
      for (j = 0; j < RecordNum[i]; j++)
      {
         pthread_rwlock_init(&(RecordLock[i][j]), NULL);
         pthread_spin_init(&(RecordLatch[i][j]), PTHREAD_PROCESS_PRIVATE);
      }
   }
}

int Hash(int table_id, TupleId r, int k)
{
   uint64_t num;
   num=RecordNum[table_id];
   if(num-1 > 0)
	   //return (int)((TupleId)(r + (TupleId)k * (1 + (TupleId)(((r >> 5) +1) % (num - 1)))) % num);
	   return k;
   else
	   return 0;
}

/*
 * return a hash value range from '0' to 'min_max-1'.
 */
int LimitHash(int table_id, TupleId r, int k, int min_max)
{
	if(min_max-1 > 0)
		return ((r%min_max + k * (1 + (((r>>5) +1) % (min_max - 1)))) % min_max);
	else
		return 0;
}

/*
 * function: to see whether a version of data item is visible, called in read operation.
 */
bool ReadMVCCVisible(Record * r, VersionId v, StartId* sid_min, CommitId* cid_min)
{
   StartId sid_max;
   CommitId cid;
   THREAD* threadinfo;

   PROC* proc;

   int index;

   //maybe the 'sid_max' can be passed as a parameter.
   threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
   index=threadinfo->index;

   proc=(PROC*)procbase+index;

   cid=r->VersionList[v].cid;

   sid_max=GetTransactionSidMax(index);

   if (cid <= sid_max)
   {
	   //add process elem lock here.
	   *sid_min=(cid > *sid_min) ? cid : *sid_min;
	   if(*cid_min < *sid_min)
		   *cid_min=*sid_min;

	   return true;
   }
   else
      return false;
}

/*
 * function: to see whether a version of data item is visible, called in update operation.
 */
bool MVCCVisible(Record * r, VersionId v)
{
   StartId sid_max;
   StartId sid_min;
   CommitId cid;
   THREAD* threadinfo;

   PROC* proc;

   int index;

   //maybe the 'sid_max' can be passed as a parameter.
   threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
   index=threadinfo->index;

   proc=(PROC*)procbase+index;

   cid=r->VersionList[v].cid;

   sid_max=GetTransactionSidMax(index);

   if (cid <= sid_max)
   {
	   //pthread_spin_lock(&ProcArrayElemLock[index]);

	   proc->sid_min=(cid > proc->sid_min) ? cid : proc->sid_min;

	   //update the 'cid_min'.
	   if(proc->cid_min < proc->sid_min)
		   proc->cid_min = proc->sid_min;

	   //pthread_spin_unlock(&ProcArrayElemLock[index]);

	   return true;
   }
   else
      return false;
}

/*
 * to see whether the version is a deleted version.
 */
bool IsMVCCDeleted(Record * r, VersionId v)
{
   if(r->VersionList[v].deleted == true)
      return true;
   else
      return false;
}

/*
 *  to see whether the transaction can update the data. return true to update, false to abort.
 */
bool IsUpdateConflict(Record * r, TransactionId tid)
{
	//self already updated the data, note that rear is not the newest version.
	VersionId newest;

	newest = (r->rear + VERSIONMAX -1) % VERSIONMAX;
	if(r->lcommit != newest)
	{
		//self already  deleted.
		if(IsMVCCDeleted(r, newest))
			return false;
		//self already updated.
		else
			return true;
	}
	//self first update the data.
	else
	{
		//update permission only when the lcommit version is visible and is not
		//a deleted version.
		if(MVCCVisible(r, r->lcommit) && !IsMVCCDeleted(r, r->lcommit))
			return true;
		else
			return false;
	}
}

/*
 * function: to find the position for a particular tuple-id in the HashTable.
 */
int BasicRecordFind(int tableid, TupleId r)
{
   int k = 0;
   int h = 0;
   uint64_t num=RecordNum[tableid];

   assert(TableList != NULL);
   THash HashTable = TableList[tableid];

   do
   {
       h = Hash(tableid, r, k);

       if (HashTable[h].tupleid == r)
          return h;
       else
          k++;
   } while (k < num);
   printf("Basic:can not find record id %ld in the table:%d! \n", r, tableid);
   return -1;
}

/*
 * function: to find the position for a particular tuple id in the HashTable, specially for TPCC benchmark.
 */
int RecordFind(int table_id, TupleId r)
{
   int k = 0;
   int h = 0;
   int w_id, d_id, o_id, bucket_id, min, max, c_id;

   int bucket_size=BucketSize[table_id];

   switch(table_id)
   {
   case Order_ID:
   case NewOrder_ID:
   case OrderLine_ID:
		w_id=(int)((r/ORDER_ID)%WHSE_ID);
		d_id=(int)((r/(ORDER_ID*WHSE_ID))%DIST_ID);
		bucket_id=(w_id-1)*10+(d_id-1);
		break;
   case Customer_ID:
   case History_ID:
	    w_id=(int)((r/CUST_ID)%WHSE_ID);
	    d_id=(int)((r/(CUST_ID*WHSE_ID))%DIST_ID);
	    bucket_id=(w_id-1)*10+(d_id-1);
   		break;
   case District_ID:
	    w_id=(int)(r%WHSE_ID);
	    bucket_id=w_id-1;
   		break;
   case Stock_ID:
   	    w_id=(int)((r/ITEM_ID)%WHSE_ID);
   	    bucket_id=w_id-1;
   		break;
   case Item_ID:
   	    bucket_id=0;
   		break;
   case Warehouse_ID:
   	    bucket_id=0;
   		break;
   default:
   	    printf("table_ID error %d\n", table_id);
   }

   min=bucket_size*bucket_id;
   max=min+bucket_size;
   assert(TableList != NULL);
   THash HashTable = TableList[table_id];

   do
   {
       h = min+LimitHash(table_id, r, k, bucket_size);
       if (HashTable[h].tupleid == r)
          return h;
       else
          k++;
   } while (k < bucket_size);
   printf("Limit:can not find record id %ld in the table:%d! \n", r, table_id);
   return -1;
}

/*
 * function: to find a position range from '0' to 'table-size' for a particular tuple id in the HashTable for insert.
 * @return:'h' for success, '-2' for already exists, '-1' for not success(already full)
 */
int BasicRecordFindHole(int tableid, TupleId r, int* flag)
{
   int k = 0;
   int h = 0;
   uint64_t num=RecordNum[tableid];

   assert(TableList != NULL);
   //HashTable is a pointer to a particular table refer to.
   THash HashTable = TableList[tableid];
   do
   {
       h = Hash(tableid, r, k);

       //find a empty record space.
       if(__sync_bool_compare_and_swap(&HashTable[h].tupleid,InvalidTupleId,r))
       {
    	   //to make sure that this place by 'h' is empty.
    	   assert(isEmptyQueue(&HashTable[h]));
    	   *flag=0;
    	   return h;
       }
       //to compare whether the two tuple_id are equal.
       else if(HashTable[h].tupleid==r)
       {
  		   printf("the data by %ld is already exist.\n",r);
  		   *flag=1;
  		   return h;
       }
       //to search the next record place.
       else
    	   k++;
   } while (k < num);
   printf("can not find a space for insert record %ld %d!\n", r, num);
   *flag=-2;
   return -2;
}

/*
 * function: to find a position in a specified bucket for a tuple, specially for TPCC benchmark.
 */
int RecordFindHole(int table_id, TupleId r, int *flag)
{
	int w_id, d_id, o_id, bucket_id, min, max;
	int bucket_size=BucketSize[table_id];
    int k = 0;
    int h = 0;
    TransactionData *tdata;
    TransactionId tid;

    bool success;

    tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);

    tid=tdata->tid;

    assert(TableList != NULL);
    THash HashTable = TableList[table_id];   //HashTable is a pointer to a particular table refer to.
    switch(table_id)
    {
    case Order_ID:
    case NewOrder_ID:
    case OrderLine_ID:
		w_id=(int)((r/ORDER_ID)%WHSE_ID);
		d_id=(int)((r/(ORDER_ID*WHSE_ID))%DIST_ID);
		bucket_id=(w_id-1)*10+(d_id-1);
    	break;
    case Customer_ID:
    case History_ID:
    	w_id=(int)((r/CUST_ID)%WHSE_ID);
    	d_id=(int)((r/(CUST_ID*WHSE_ID))%DIST_ID);
    	bucket_id=(w_id-1)*10+(d_id-1);
    	break;
    case District_ID:
    	w_id=(int)(r%WHSE_ID);
    	bucket_id=w_id-1;
    	break;
    case Stock_ID:
    	w_id=(int)((r/ITEM_ID)%WHSE_ID);
    	bucket_id=w_id-1;
    	break;
    case Item_ID:
    	bucket_id=0;
    	break;
    case Warehouse_ID:
    	bucket_id=0;
    	break;
    default:
    	printf("table_ID error %d\n", table_id);
    }

	min=bucket_size*bucket_id;
	max=min+bucket_size;

	//loop here to find a empty position.
    do
    {
	    h = min+LimitHash(table_id, r, k, bucket_size);

	    pthread_spin_lock(&RecordLatch[table_id][h]);

	    if(HashTable[h].tupleid == InvalidTupleId)
	    {

	    	if(!isEmptyQueue(&HashTable[h]))
	    	{
	    		printf("find:assert(isEmptyQueue(&HashTable[h])):tid:%d, table_id:%d, tuple_id:%ld, h:%d, %ld, %d, %d, %d\n",tdata->tid, table_id, r, h, HashTable[h].tupleid, HashTable[h].front, HashTable[h].lcommit, HashTable[h].rear);
	    		PrintTable(table_id);
	    		exit(-1);
	    	}
	    	if(r == InvalidTupleId)
	    	{
	    		printf("r is InvalidTupleId: table_id=%d, tuple_id=%ld\n",table_id, r);
	    		exit(-1);
	    	}

	    	HashTable[h].tupleid=r;

	    	pthread_spin_unlock(&RecordLatch[table_id][h]);
	    	success=true;
	    }
	    else
	    {
	    	pthread_spin_unlock(&RecordLatch[table_id][h]);
	    	success=false;
	    }

	    //find a empty record space.
	    if(success == true)
	    {
		    *flag=0;
		    return h;
	    }
	    //to compare whether the two tuple_id are equal.
	    else if(HashTable[h].tupleid==r)
	    {
		    *flag=1;
		    return h;
	    }
	    //to search the next record place.
	    else
		   k++;
    } while (k < bucket_size);
    *flag=-2;
    return -2;
}

/*
 * function: insert a tuple.
 * @return: '0' to rollback, '1' to go head.
 */
int Data_Insert(int table_id, TupleId tuple_id, TupleId value)
{
	int index=0;
	int h;
	int flag;

	void* data=NULL;

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

	//get the index of 'tuple_id' in table 'table_id'.
	THash HashTable = TableList[table_id];

	//find an empty place.
	h=RecordFindHole(table_id, tuple_id, &flag);

	if(flag==-2)
	{
		//no space for new tuple to insert.
		printf("Data_insert: flag==-1.\n");
		printf("no space for table_id:%d, tuple_id:%d\n",table_id, tuple_id);
		exit(-1);
		return 0;
	}
	else if(flag==1 && IsInsertDone(table_id, h))
	{
		//tuple by (table_id, tuple_id) already exists, and
		//insert already done, so return to rollback.
		return 0;
	}

	//not to truly insert the tuple, just record the insert operation, truly
	//insert until the transaction commits.
	datard.type=DataInsert;

	datard.table_id=table_id;
	datard.tuple_id=tuple_id;
	datard.value=value;
	datard.index=h;
	DataRecordInsert(&datard);
	return 1;
}

/*
 * function: update a tuple.
 * @return:'0' for not found, '1' for success.
 */
int Data_Update(int table_id, TupleId tuple_id, TupleId value)
{
	int index=0;
	int h,i;

	void* data=NULL;

	DataRecord datard;

	TransactionData* tdata;
	TransactionId tid;
	THREAD* threadinfo;

	int result;

	int flag;

	bool abort;

	TupleId tempid;

	//get the pointer to current transaction data.
	tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
	tid=tdata->tid;

	//get the pointer to current thread information.
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index=threadinfo->index;

	//get the index of 'tuple_id' in table 'table_id'.
	THash HashTable = TableList[table_id];

	//find the place by 'tuple_id'
	h = RecordFind(table_id, tuple_id);

	//not found.
	if (h < 0)
	{
		//abort transaction outside the function.
		return 0;
	}

	//light-data-read operation before holding write lock to detect write-write conflicts between transactions.
	result=Light_Data_Read(table_id, h);
	if(result==0)
	{
		//return to abort.
		return -1;
	}

	datard.type=DataUpdate;

	datard.table_id=table_id;
	datard.tuple_id=tuple_id;

	datard.value=value;

	datard.index=h;
	DataRecordInsert(&datard);
	return 1;
}

/*
 * function: delete a tuple.
 * @return:'0' for not found, '1' for success.
 */
int Data_Delete(int table_id, TupleId tuple_id)
{
	int index=0;
	int h,i;

	void* data=NULL;

	DataRecord datard;

	TransactionData* tdata;
	TransactionId tid;
	THREAD* threadinfo;

	int result;
	int flag;

	bool abort;
	TupleId tempid;

	//get the pointer to current transaction data.
	tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
	tid=tdata->tid;

	//get the pointer to current thread information.
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index=threadinfo->index;

	//get the index of 'tuple_id' in table 'table_id'.
	THash HashTable = TableList[table_id];
	h = RecordFind(table_id, tuple_id);

	//not found.
	if(h < 0)
	{
		return 0;
	}

	//light-data-read operation before holding write lock to detect write-write conflicts between transactions.
	result=Light_Data_Read(table_id, h);
	if(result==0)
	{
		//return to abort.
		return -1;
	}

	//record the deleted data.
	//get the data pointer.
	datard.type=DataDelete;

	datard.table_id=table_id;
	datard.tuple_id=tuple_id;

	datard.index=h;
	DataRecordInsert(&datard);
	return 1;
}

/*
 * function: read a tuple.
 * @return:0 for read nothing, to rollback or just let it go, else return 'value'.
 */
TupleId Data_Read(int table_id, TupleId tuple_id, int* flag)
{
	VersionId i;
	int h;
	int index;
	TupleId visible;

	int result;

	VersionId newread,newest;
	//Version* v;
	char* DataMemStart=NULL;

	TransactionData* tdata;
	TransactionId tid;
	THREAD* threadinfo;

	StartId sid_min;
	CommitId cid_min;

	*flag=1;
	//get the pointer to current transaction data.
	tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
	tid=tdata->tid;

	//get the pointer to current thread information.
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index=threadinfo->index;

	DataMemStart=(char*)pthread_getspecific(DataMemKey);

	THash HashTable = TableList[table_id];

	h = RecordFind(table_id, tuple_id);

	//not found.
	if(h < 0)
	{
		//to rollback.
		*flag=0;
		return 0;
	}

	//the data by 'tuple_id' exist, so to read it.

	//we should add to the read list before reading.
	ReadListInsert(table_id, h, tid, index);

	//record the read-write anti-dependency in the conflict transactions table.
	result=ReadCollusion(table_id, h, tid,index);

	if(result==0)
	{
		//current transaction has to rollback.
		*flag=-3;
		return 0;
	}

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
		*flag=-1;
		return 0;
	}
	else if(visible > 0)
	{
		//see own transaction's update.
		return visible;
	}

	pthread_spin_lock(&RecordLatch[table_id][h]);
	//by here, we try to read already committed tuple.
	if(HashTable[h].lcommit >= 0)
	{
		sid_min=GetTransactionSidMin(index);
		cid_min=GetTransactionCidMin(index);
		for (i = HashTable[h].lcommit; i != (HashTable[h].front + VERSIONMAX - 1) % VERSIONMAX; i = (i + VERSIONMAX - 1) % VERSIONMAX)
		{

			if (ReadMVCCVisible(&(HashTable[h]), i, &sid_min, &cid_min))
			{
				if(IsMVCCDeleted(&HashTable[h],i))
				{
					pthread_spin_unlock(&RecordLatch[table_id][h]);
					MVCCUpdateProcId(index, sid_min, cid_min);
					*flag=-2;
					return 0;
				}
				else
				{
					pthread_spin_unlock(&RecordLatch[table_id][h]);
					MVCCUpdateProcId(index, sid_min, cid_min);
					return HashTable[h].VersionList[i].value;
				}

			}
		}
	}
	pthread_spin_unlock(&RecordLatch[table_id][h]);

	//can see nothing of the data.
	*flag=-3;
	return 0;
}

/*
 * function: read a tuple in specified position.
 */
TupleId LocateData_Read(int table_id, int h, TupleId *id, bool* abort)
{
	VersionId i;
	int index, c_id;
	TupleId visible, tuple_id;

	int result;

	VersionId newread,newest;
	//Version* v;
	char* DataMemStart=NULL;

	TransactionData* tdata;
	TransactionId tid;
	THREAD* threadinfo;

	int count;

	StartId sid_min;
	CommitId cid_min;

	*abort=false;
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
	ReadListInsert(table_id, h, tid, index);

	result=ReadCollusion(table_id, h, tid,index);
	if(result==0)
	{
		//current transaction has to rollback.
		abort=true;
		return 0;
	}

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
		return 0;
	}
	else if(visible > 0)
	{
		//see own transaction's update.
		*id=tuple_id;
		return visible;
	}

	pthread_spin_lock(&RecordLatch[table_id][h]);
	//by here, we try to read already committed tuple.
	if(HashTable[h].lcommit >= 0)
	{
		sid_min=GetTransactionSidMin(index);
		cid_min=GetTransactionCidMin(index);
		for (i = HashTable[h].lcommit; i != (HashTable[h].front + VERSIONMAX - 1) % VERSIONMAX; i = (i + VERSIONMAX - 1) % VERSIONMAX)
		{
			if (ReadMVCCVisible(&(HashTable[h]), i, &sid_min, &cid_min) )
			{
				if(IsMVCCDeleted(&HashTable[h],i))
				{
					pthread_spin_unlock(&RecordLatch[table_id][h]);
					MVCCUpdateProcId(index, sid_min, cid_min);
					return 0;
				}
				else
				{
					pthread_spin_unlock(&RecordLatch[table_id][h]);
					MVCCUpdateProcId(index, sid_min, cid_min);
					*id=tuple_id;
					return HashTable[h].VersionList[i].value;
				}
			}
		}
	}
	pthread_spin_unlock(&RecordLatch[table_id][h]);

	return 0;


}

/*
 * function: used for read operation in update and delete operation, while
 * there is no need to access the data of the tuple.
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

/*
 * function: to see whether the tuple has been inserted by one committed transaction.
 * @return:'true' means the tuple in 'index' has been inserted, 'false' for else.
 */
bool IsInsertDone(int table_id, int index)
{
	THash HashTable = TableList[table_id];
	bool done;

	pthread_spin_lock(&RecordLatch[table_id][index]);

	//maybe there is need to change.
	if(HashTable[index].lcommit >= 0)done=true;
	else done=false;

	pthread_spin_unlock(&RecordLatch[table_id][index]);

	return done;
}

/*
 * in order to avoid dead-lock when updating one tuple concurrently, we put physically 'insert', 'update' and
 * 'delete' operations at the end of one transaction, to see following function 'TrulyDataInsert',
 * 'TrulyDataUpdate' and 'TrulyDataDelete'.
 */

/*
 * @return:'1' for success, '-1' for rollback.
 */
int TrulyDataInsert(int table_id, int index, TupleId tuple_id, TupleId value)
{
	void* lock=NULL;
	TransactionData* tdata;
	TransactionId tid;
	DataLock lockrd;
	int proc_index;
	THREAD* threadinfo;

	THash HashTable=TableList[table_id];

	tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
	tid=tdata->tid;

	//get the pointer to current thread information.
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	proc_index=threadinfo->index;

	pthread_rwlock_wrlock(&(RecordLock[table_id][index]));

	if(IsInsertDone(table_id, index))
	{
		//other transaction has inserted the tuple.
		pthread_rwlock_unlock(&(RecordLock[table_id][index]));
		return -1;
	}

	WriteListInsert(table_id, index, tid);

	//by here, we can insert the tuple.
	lock = (void *) (&(RecordLock[table_id][index]));
	//record the lock.
	lockrd.table_id=table_id;
	lockrd.tuple_id=tuple_id;
	lockrd.lockmode=LOCK_EXCLUSIVE;
	lockrd.ptr=lock;
	DataLockInsert(&lockrd);

	pthread_spin_lock(&RecordLatch[table_id][index]);

	if(HashTable[index].lcommit >= 0)
	{
		//other transaction has inserted the tuple successfully.
		pthread_spin_unlock(&RecordLatch[table_id][index]);
		printf("wrlock error TID:%d, table_id:%d, %ld, %ld, %d\n",tid, table_id, tuple_id, HashTable[index].tupleid, HashTable[index].VersionList[0].tid);
		exit(-1);
	}

	//current transaction can insert the tuple.
	assert(isEmptyQueue(&HashTable[index]));
	HashTable[index].tupleid=tuple_id;
	EnQueue(&HashTable[index],tid, value);
	pthread_spin_unlock(&RecordLatch[table_id][index]);

	return 1;
}

/*
 * @return:'1' for success, '-1' for rollback.
 */
int TrulyDataUpdate(int table_id, int index, TupleId tuple_id, TupleId value)
{
	int old,i;
	bool firstadd=false;
	void* lock=NULL;
	TransactionData* tdata;
	TransactionId tid;
	DataLock lockrd;
	int proc_index;
	THREAD* threadinfo;

	THash HashTable=TableList[table_id];

	tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
	tid=tdata->tid;

	//get the pointer to current thread information.
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	proc_index=threadinfo->index;

	//to void repeatedly add lock.
	if(!IsWrLockHolding(table_id,tuple_id))
	{
		//the first time to hold the wr-lock on data (table_id,tuple_id).
		pthread_rwlock_wrlock(&(RecordLock[table_id][index]));
		firstadd=true;
	}

	//by here, we have hold the write-lock.

	//current transaction can't update the data.
	if(!IsUpdateConflict(&HashTable[index],tid))
	{
		//release the write-lock and return to rollback.
		if(firstadd)
			pthread_rwlock_unlock(&(RecordLock[table_id][index]));
		return -1;

	}
	WriteListInsert(table_id, index, tid);

	MergeReadList(table_id, index);

	//by here, we are sure that we can update the data.
	lock = (void *) (&(RecordLock[table_id][index]));

	//record the lock.
	lockrd.table_id=table_id;
	lockrd.tuple_id=tuple_id;
	lockrd.lockmode=LOCK_EXCLUSIVE;
	lockrd.ptr=lock;
	DataLockInsert(&lockrd);

	//here are the place we truly update the data.

	pthread_spin_lock(&RecordLatch[table_id][index]);

	assert(HashTable[index].tupleid == tuple_id);
	assert(!isEmptyQueue(&HashTable[index]));

	EnQueue(&HashTable[index],tid, value);
	if (isFullQueue(&(HashTable[index])))
	{
	   old = (HashTable[index].front +  VERSIONMAX/3) % VERSIONMAX;
	   for (i = HashTable[index].front; i != old; i = (i+1) % VERSIONMAX)
	   {
			HashTable[index].VersionList[i].cid = 0;
			HashTable[index].VersionList[i].tid = 0;
			HashTable[index].VersionList[i].deleted = false;

			HashTable[index].VersionList[i].value= 0;
		}
		HashTable[index].front = old;
	}
	pthread_spin_unlock(&RecordLatch[table_id][index]);

	return 1;
}

/*
 * @return:'1' for success, '-1' for rollback.
 */
int TrulyDataDelete(int table_id, int index, TupleId tuple_id)
{
	int old,i;
	bool firstadd=false;
	VersionId newest;
	void* lock=NULL;
	TransactionData* tdata;
	TransactionId tid;
	DataLock lockrd;
	int proc_index;
	THREAD* threadinfo;

	THash HashTable=TableList[table_id];

	tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
	tid=tdata->tid;

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	proc_index=threadinfo->index;

	//to void repeatedly add lock.
	if(!IsWrLockHolding(table_id,tuple_id))
	{
		//the first time to hold the wr-lock on data (table_id,tuple_id).
		pthread_rwlock_wrlock(&(RecordLock[table_id][index]));
		firstadd=true;
	}

	//by here, we have hold the write-lock.

	//current transaction can't update the data.
	if(!IsUpdateConflict(&HashTable[index],tid))
	{
		//release the write-lock and return to rollback.
		if(firstadd)
			pthread_rwlock_unlock(&(RecordLock[table_id][index]));
		return -1;

	}

	WriteListInsert(table_id, index, tid);

	MergeReadList(table_id, index);

	//by here, we are sure that we can update the data.
	lock = (void *) (&(RecordLock[table_id][index]));

	//record the lock.
	lockrd.table_id=table_id;
	lockrd.tuple_id=tuple_id;
	lockrd.lockmode=LOCK_EXCLUSIVE;
	lockrd.ptr=lock;
	DataLockInsert(&lockrd);

	//here are the place we truly delete the data.
	pthread_spin_lock(&RecordLatch[table_id][index]);
	assert(HashTable[index].tupleid == tuple_id);
	assert(!isEmptyQueue(&HashTable[index]));

	EnQueue(&HashTable[index],tid, 0);
	newest = (HashTable[index].rear + VERSIONMAX -1) % VERSIONMAX;
	HashTable[index].VersionList[newest].deleted = true;

	if (isFullQueue(&(HashTable[index])))
	{
	   old = (HashTable[index].front +  VERSIONMAX/3) % VERSIONMAX;
	   for (i = HashTable[index].front; i != old; i = (i+1) % VERSIONMAX)
	   {
		    HashTable[index].VersionList[i].cid = 0;
			HashTable[index].VersionList[i].tid = 0;
			HashTable[index].VersionList[i].deleted = false;

			HashTable[index].VersionList[i].value = 0;
		}
		HashTable[index].front = old;
	}

	pthread_spin_unlock(&RecordLatch[table_id][index]);

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
	strcat(filename, ".txt");

	if((fp=fopen(filename,"w"))==NULL)
	{
		printf("file open error\n");
		exit(-1);
	}

	i=table_id;

	HashTable=TableList[i];
	for(j=0;j<RecordNum[i];j++)
	{
		rd=&HashTable[j];
		fprintf(fp,"%d: %ld",j,rd->tupleid);
		for(k=0;k<VERSIONMAX;k++)
			fprintf(fp,"(%2d %d %ld %2d)",rd->VersionList[k].tid,rd->VersionList[k].cid,rd->VersionList[k].value,rd->VersionList[k].deleted);
		fprintf(fp,"\n");
	}
	printf("\n");
}

void validation(int table_id)
{
	THash HashTable;
	uint64_t i;
	int count=0;

	HashTable=TableList[table_id];

	for(i=0;i<RecordNum[table_id];i++)
	{
		if(HashTable[i].tupleid == InvalidTupleId)
			count++;
	}
	printf("table: %d of %ld rows are available.\n",count, RecordNum[table_id]);
}

void PrimeBucketSize(void)
{
	int i, j;
	i=0, j=0;
	for(i=0;i<TABLENUM;i++)
	{
		j=0;
		while(BucketSize[i] > Prime[j])
		{
			j++;
		}
		BucketSize[i]=Prime[j];
	}
}

void ReadPrimeTable(void)
{
	FILE* fp;
	int i, num;
	if((fp=fopen("prime.txt","r"))==NULL)
	{
		printf("file open error.\n");
		exit(-1);
	}

	//printf("file open succeed.\n");
	i=0;
	while(fscanf(fp,"%d",&num) > 0)
	{
		//printf("%d\n",num);
		Prime[i++]=num;
	}
	PrimeNum=i;
	fclose(fp);
	//printf("end read prime table\n");
}
