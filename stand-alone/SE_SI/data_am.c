/*
 * data_am.c
 *
 *  Created on: Dec 7, 2015
 *      Author: xiaoxin
 */
/*
 * interface for data access method.
 */
#include <pthread.h>
#include <assert.h>
#include <stdbool.h>

#include "config.h"
#include "data_am.h"
#include "data_record.h"
#include "lock_record.h"
#include "thread_global.h"
#include "proc.h"
#include "trans.h"
#include "snapshot.h"
#include "transactions.h"

//rw-lock on each tuple.
pthread_rwlock_t* RecordLock[TABLENUM];

//spin-lock on each tuple to ensure atomic read and write.
pthread_spinlock_t* RecordLatch[TABLENUM];

Record* TableList[TABLENUM];

static bool IsUpdateConflict(Record * r, TransactionId tid, Snapshot* snap);

static bool IsInsertDone(int table_id, int index);

static void PrimeBucketSize(void);

static void ReadPrimeTable(void);

int BucketNum[TABLENUM];
int BucketSize[TABLENUM];
int RecordNum[TABLENUM];

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
   // 'lcommit' means that the last version-id committed, it should be initialized to '-1' for empty tuple.
   r->lcommit = -1;
   for (i = 0; i < VERSIONMAX; i++)
   {
      r->VersionList[i].tid = 0;
      r->VersionList[i].committime = InvalidTimestamp;
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
   r->VersionList[r->rear].value=value;

   r->rear = (r->rear + 1) % VERSIONMAX;
}

/*
 * initialize the number of buckets and the size of each bucket for each table.
 */
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
	//the limited max number of items for each order is 10.
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
 * function: to determine whether a version of the tuple is visible according to the snapshot 'snap'.
 * @return:'true' for visible, 'false' for invisible.
 */
bool MVCCVisible(Record * r, VersionId v, Snapshot* snap)
{
	TransactionId tid;

	tid=r->VersionList[v].tid;

	//if 'tid' is not in snapshot 'snap', then transaction by 'tid' must
	//have committed,
	if(!TidInSnapshot(tid, snap))
		return true;
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
bool IsUpdateConflict(Record * r, TransactionId tid, Snapshot* snap)
{
	VersionId newest;

	newest = (r->rear + VERSIONMAX -1) % VERSIONMAX;

	if(r->lcommit != newest)
	{
		assert(r->VersionList[newest].tid == tid);
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
		//update permitted only when the 'lcommit' version is visible and is not
		//a deleted version.
		if(MVCCVisible(r, r->lcommit, snap) && !IsMVCCDeleted(r, r->lcommit))
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
   int w_id, d_id, bucket_id, min, max;
   int offset=-1;

   int bucket_size=BucketSize[table_id];

   switch(table_id)
   {
   case Order_ID:
   case NewOrder_ID:
		w_id=(int)((r/ORDER_ID)%WHSE_ID);
		d_id=(int)((r/(ORDER_ID*WHSE_ID))%DIST_ID);
		bucket_id=(w_id-1)*10+(d_id-1);

		offset=(int)(r%ORDER_ID);
		break;
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

	    offset=(int)(r%CUST_ID);
   		break;
   case District_ID:
	    w_id=(int)(r%WHSE_ID);
	    bucket_id=w_id-1;

	    offset=(int)((r/WHSE_ID)%DIST_ID);
   		break;
   case Stock_ID:
   	    w_id=(int)((r/ITEM_ID)%WHSE_ID);
   	    bucket_id=w_id-1;

   	 offset=(int)(r%ITEM_ID);
   		break;
   case Item_ID:
   	    bucket_id=0;

   	    offset=(int)r;
   		break;
   case Warehouse_ID:
   	    bucket_id=0;

   	    offset=(int)r;
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
   printf("Limit:can not find record id %ld in the table:%d, bucketsize=%d! \n", r, table_id, bucket_size);
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
   printf("can not find a space for insert record %ld %ld!\n", r, num);
   *flag=-2;
   return -2;
}

/*
 * function: to find a position in a specified bucket for a tuple, specially for TPCC benchmark.
 */
int RecordFindHole(int table_id, TupleId r, int *flag)
{
	int w_id, d_id, bucket_id, min, max;
	int bucket_size=BucketSize[table_id];
    int k = 0;
    int h = 0;

    int offset=-1;
    TransactionData *tdata;
    bool success;

    tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);

    assert(TableList != NULL);

    //HashTable is a pointer to a particular table refer to.
    THash HashTable = TableList[table_id];
    switch(table_id)
    {
    case Order_ID:
    case NewOrder_ID:
		w_id=(int)((r/ORDER_ID)%WHSE_ID);
		d_id=(int)((r/(ORDER_ID*WHSE_ID))%DIST_ID);
		bucket_id=(w_id-1)*10+(d_id-1);

		offset=(int)(r%ORDER_ID);
    	break;
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

    	offset=(int)(r%CUST_ID);
    	break;
    case District_ID:
    	w_id=(int)(r%WHSE_ID);
    	bucket_id=w_id-1;

    	offset=(int)((r/WHSE_ID)%DIST_ID);
    	break;
    case Stock_ID:
    	w_id=(int)((r/ITEM_ID)%WHSE_ID);
    	bucket_id=w_id-1;

    	offset=(int)(r%ITEM_ID);
    	break;
    case Item_ID:
    	bucket_id=0;

    	offset=(int)r;
    	break;
    case Warehouse_ID:
    	bucket_id=0;

    	offset=(int)r;
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
	    	HashTable[h].tupleid=r;
	    	pthread_spin_unlock(&RecordLatch[table_id][h]);
	    	success=true;
	    }
	    else
	    {
	    	pthread_spin_unlock(&RecordLatch[table_id][h]);
	    	success=false;
	    }

	    //succeed in finding a empty position.
	    if(success == true)
	    {
		    //to make sure that this place by 'h' is empty.
		    assert(isEmptyQueue(&HashTable[h]));

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
	int h,flag;
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
	h=RecordFindHole(table_id, tuple_id, &flag);

	if(flag==-2)
	{
		//no space for new tuple to insert.
		return 0;
	}
	else if(flag==1 && IsInsertDone(table_id, h))
	{
		//tuple by (table_id, tuple_id) already exists, and
		//insert already done, so return to rollback.
		return 0;
	}

	//record the inserted data 'datard'.
	//get the data pointer.
	data = (void *) (&HashTable[h]);
	datard.type=DataInsert;
	datard.data=data;

	datard.table_id=table_id;
	datard.tuple_id=tuple_id;

	datard.value=value;
	datard.index=h;
	DataRecordInsert(&datard);

	return 1;
}

/*
 * function: update a tuple.
 * @return:'0' for not found, '1' for success, '-1' for update-conflict-rollback.
 */
int Data_Update(int table_id, TupleId tuple_id, TupleId value)
{
	int index=0;
	int h;
	void* data=NULL;
	DataRecord datard;
	Snapshot* snap;

	TransactionData* tdata;
	TransactionId tid;
	THREAD* threadinfo;

	//get the pointer to current transaction data.
	tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
	tid=tdata->tid;

	//get the pointer to current thread information.
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index=threadinfo->index;

	//get the pointer to transaction-snapshot-data.
	snap=(Snapshot*)pthread_getspecific(SnapshotDataKey);

	//get the index of 'tuple_id' in table 'table_id'.
	THash HashTable = TableList[table_id];
	h = RecordFind(table_id, tuple_id);

	//not found.
	if (h < 0)
	{
		//abort transaction outside the function.
		return 0;
	}

	//record the updated data.
	//get the data pointer.
	data = (void *) (&HashTable[h]);

	datard.type=DataUpdate;
	datard.data=data;

	datard.table_id=table_id;
	datard.tuple_id=tuple_id;

	datard.value=value;

	datard.index=h;
	DataRecordInsert(&datard);
	return 1;
}

/*
 * function: delete a tuple.
 * @return:'0' for not found, '1' for success, '-1' for update-conflict-rollback.
 */
int Data_Delete(int table_id, TupleId tuple_id)
{
	int index=0;
	int h;
	void* data=NULL;
	DataRecord datard;
	Snapshot* snap;

	TransactionData* tdata;
	TransactionId tid;
	THREAD* threadinfo;
	//get the pointer to current transaction data.
	tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
	tid=tdata->tid;

	//get the pointer to current thread information.
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index=threadinfo->index;

	//get pointer to transaction-snapshot-data.
	snap=(Snapshot*)pthread_getspecific(SnapshotDataKey);

	//get the index of 'tuple_id' in table 'table_id'.
	THash HashTable = TableList[table_id];
	h = RecordFind(table_id, tuple_id);

	//not found.
	if(h < 0)
	{
		return 0;
	}

	//record the deleted data.
	//get the data pointer.
	data = (void *) (&HashTable[h]);
	datard.type=DataDelete;
	datard.data=data;

	datard.table_id=table_id;
	datard.tuple_id=tuple_id;

	datard.index=h;
	DataRecordInsert(&datard);
	return 1;
}

/*
 * function: read a tuple.
 * @return:NULL for read nothing, to rollback or just let it go.
 */
TupleId Data_Read(int table_id, TupleId tuple_id, int* flag)
{
	VersionId i;
	int h;
	int index, visible;
	VersionId newest;
	Snapshot* snap;
	char* DataMemStart;

	*flag=1;

	TransactionData* tdata;
	TransactionId tid;
	THREAD* threadinfo;
	//get the pointer to current transaction data.
	tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
	tid=tdata->tid;

	//get the pointer to current thread information.
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index=threadinfo->index;

	//get the pointer to data-memory.
	DataMemStart=(char*)pthread_getspecific(DataMemKey);

	//get pointer to transaction-snapshot-data.
	snap=(Snapshot*)pthread_getspecific(SnapshotDataKey);

	THash HashTable = TableList[table_id];
	h = RecordFind(table_id, tuple_id);

	//not found.
	if(h < 0)
	{
		//to rollback.
		*flag=0;
		return 0;
	}

	//the data by 'tuple_id' exist, so read it.
	newest = (HashTable[h].rear + VERSIONMAX -1) % VERSIONMAX;

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
		for (i = HashTable[h].lcommit; i != (HashTable[h].front + VERSIONMAX - 1) % VERSIONMAX; i = (i + VERSIONMAX - 1) % VERSIONMAX)
		{
			if (MVCCVisible(&(HashTable[h]), i, snap) )
			{
				if(IsMVCCDeleted(&HashTable[h],i))
				{
					pthread_spin_unlock(&RecordLatch[table_id][h]);
					*flag=-2;
					return 0;
				}
				else
				{
					pthread_spin_unlock(&RecordLatch[table_id][h]);
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
TupleId LocateData_Read(int table_id, int h, TupleId *id)
{
	VersionId i;
	int index;
	TupleId visible, tuple_id;
	VersionId newest;
	Snapshot* snap;
	char* DataMemStart;

	TransactionData* tdata;
	TransactionId tid;
	THREAD* threadinfo;
	//get the pointer to current transaction data.
	tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
	tid=tdata->tid;

	//get the pointer to current thread information.
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index=threadinfo->index;

	//get the pointer to data-memory.
	DataMemStart=(char*)pthread_getspecific(DataMemKey);

	//get pointer to transaction-snapshot-data.
	snap=(Snapshot*)pthread_getspecific(SnapshotDataKey);

	THash HashTable = TableList[table_id];

	if(HashTable[h].tupleid == InvalidTupleId)
	{
		return 0;
	}

	tuple_id=HashTable[h].tupleid;

	//the data by 'tuple_id' exist, so to read it.
	pthread_spin_lock(&RecordLatch[table_id][h]);
	newest = (HashTable[h].rear + VERSIONMAX -1) % VERSIONMAX;
	//the data by (table_id,tuple_id) is being updated by other transaction.
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
		*id=tuple_id;
		return visible;
	}

	//by here, we try to read already committed tuple.
	if(HashTable[h].lcommit >= 0)
	{
		for (i = HashTable[h].lcommit; i != (HashTable[h].front + VERSIONMAX - 1) % VERSIONMAX; i = (i + VERSIONMAX -1) % VERSIONMAX)
		{
			if (MVCCVisible(&(HashTable[h]), i, snap) )
			{
				if(IsMVCCDeleted(&HashTable[h],i))
				{
					pthread_spin_unlock(&RecordLatch[table_id][h]);
					return 0;
				}
				else
				{
					pthread_spin_unlock(&RecordLatch[table_id][h]);
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
 * function: to see whether the tuple has been inserted by one committed transaction.
 * @return:'true' means the tuple in 'index' has been inserted, 'false' for else.
 */
bool IsInsertDone(int table_id, int index)
{
	THash HashTable = TableList[table_id];
	bool done;

	pthread_spin_lock(&RecordLatch[table_id][index]);

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
int TrulyDataInsert(void* data, int table_id, int index, TupleId tuple_id, TupleId value)
{
	void* lock=NULL;
	TransactionData* tdata;
	TransactionId tid;
	DataLock lockrd;

	THash HashTable=TableList[table_id];

	tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
	tid=tdata->tid;

	pthread_rwlock_wrlock(&(RecordLock[table_id][index]));

	if(IsInsertDone(table_id, index))
	{
		//other transaction has inserted the tuple.
		pthread_rwlock_unlock(&(RecordLock[table_id][index]));
		return -1;
	}

	//by here, we can insert the tuple.
	lock = (void *) (&(RecordLock[table_id][index]));
	//record the lock.
	lockrd.table_id=table_id;
	lockrd.tuple_id=tuple_id;
	lockrd.lockmode=LOCK_EXCLUSIVE;
	lockrd.ptr=lock;
	DataLockInsert(&lockrd);

	pthread_spin_lock(&RecordLatch[table_id][index]);

	assert(tuple_id != InvalidTupleId);
	assert(isEmptyQueue(&HashTable[index]));

	HashTable[index].tupleid = tuple_id;
	EnQueue(&HashTable[index],tid,value);

	pthread_spin_unlock(&RecordLatch[table_id][index]);

	return 1;
}

/*
 * @return:'1' for success, '-1' for rollback.
 */
int TrulyDataUpdate(void* data, int table_id, int index, TupleId tuple_id, TupleId value)
{
	int old,i;
	bool firstadd=false;
	void* lock=NULL;
	TransactionData* tdata;
	TransactionId tid;
	DataLock lockrd;
	Snapshot* snap;

	THash HashTable=TableList[table_id];

	tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
	tid=tdata->tid;

	//get the pointer to transaction-snapshot-data.
	snap=(Snapshot*)pthread_getspecific(SnapshotDataKey);

	//to void repeatedly add lock.
	if(!IsWrLockHolding(table_id,tuple_id))
	{
		//the first time to hold the wr-lock on data (table_id,tuple_id).
		pthread_rwlock_wrlock(&(RecordLock[table_id][index]));
		firstadd=true;
	}

	//by here, we have hold the write-lock.

	//current transaction can't update the data.
	if(!IsUpdateConflict(&HashTable[index],tid,snap))
	{
		//release the write-lock and return to rollback.
		if(firstadd)
			pthread_rwlock_unlock(&(RecordLock[table_id][index]));
		return -1;

	}

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
	assert(!isEmptyQueue(&HashTable[index]));

	EnQueue(&HashTable[index],tid,value);
	if (isFullQueue(&(HashTable[index])))
	{
	   old = (HashTable[index].front +  VERSIONMAX/3) % VERSIONMAX;
	   for (i = HashTable[index].front; i != old; i = (i+1) % VERSIONMAX)
	   {
			HashTable[index].VersionList[i].committime = InvalidTimestamp;
			HashTable[index].VersionList[i].tid = 0;
			HashTable[index].VersionList[i].deleted = false;

			HashTable[index].VersionList[i].value=0;
		}
		HashTable[index].front = old;
	}
	pthread_spin_unlock(&RecordLatch[table_id][index]);

	return 1;
}

/*
 * @return:'1' for success, '-1' for rollback.
 */
int TrulyDataDelete(void* data, int table_id, int index, TupleId tuple_id)
{
	int old,i;
	bool firstadd=false;
	VersionId newest;
	void* lock=NULL;
	TransactionData* tdata;
	TransactionId tid;
	DataLock lockrd;
	Snapshot* snap;

	THash HashTable=TableList[table_id];

	tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
	tid=tdata->tid;

	//get the pointer to transaction-snapshot-data.
	snap=(Snapshot*)pthread_getspecific(SnapshotDataKey);


	//to void repeatedly add lock.
	if(!IsWrLockHolding(table_id,tuple_id))
	{
		//the first time to hold the wr-lock on data (table_id,tuple_id).
		pthread_rwlock_wrlock(&(RecordLock[table_id][index]));
		firstadd=true;
	}

	//by here, we have hold the write-lock.

	//current transaction can't update the data.
	if(!IsUpdateConflict(&HashTable[index],tid,snap))
	{
		//release the write-lock and return to rollback.
		if(firstadd)
			pthread_rwlock_unlock(&(RecordLock[table_id][index]));
		return -1;

	}

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
	assert(!isEmptyQueue(&HashTable[index]));

	EnQueue(&HashTable[index],tid,0);
	newest = (HashTable[index].rear + VERSIONMAX -1) % VERSIONMAX;
	HashTable[index].VersionList[newest].deleted = true;

	if (isFullQueue(&(HashTable[index])))
	{
	   old = (HashTable[index].front +  VERSIONMAX/3) % VERSIONMAX;
	   for (i = HashTable[index].front; i != old; i = (i+1) % VERSIONMAX)
	   {
		    HashTable[index].VersionList[i].committime = InvalidTimestamp;
			HashTable[index].VersionList[i].tid = 0;
			HashTable[index].VersionList[i].deleted = false;

			HashTable[index].VersionList[i].value=0;
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
			fprintf(fp,"(%2d %ld %ld %2d)",rd->VersionList[k].tid,rd->VersionList[k].committime,rd->VersionList[k].value,rd->VersionList[k].deleted);
		fprintf(fp,"\n");
	}
	printf("\n");
}

void PrimeBucketSize(void)
{
	int i, j;
	i=0, j=0;
	for(i=0;i<TABLENUM;i++)
	{
		j=0;
		while(BucketSize[i] > Prime[j] && j < PrimeNum)
		{
			j++;
		}
		if(j < PrimeNum)
			BucketSize[i]=Prime[j];
		printf("BucketSize:%d , %d\n",i, BucketSize[i]);
	}
}

void ReadPrimeTable(void)
{
	printf("begin read prime table\n");
	FILE* fp;
	int i, num;
	if((fp=fopen("prime.txt","r"))==NULL)
	{
		printf("file open error.\n");
		exit(-1);
	}

	i=0;
	while(fscanf(fp,"%d",&num) > 0)
	{
		Prime[i++]=num;
	}
	PrimeNum=i;
	fclose(fp);
}




