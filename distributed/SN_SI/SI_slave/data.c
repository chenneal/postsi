/*
 * data.c
 *
 *  Created on: Jan 29, 2016
 *      Author: Yu
 */

#include <pthread.h>
#include <assert.h>
#include "config.h"
#include "communicate.h"
#include "timestamp.h"
#include "data.h"
#include "snapshot.h"
#include "config.h"
#include "transactions.h"
#include "trans.h"
#include "thread_global.h"

static bool IsInsertDone(int table_id, int index);

static void PrimeBucketSize(void);

static void ReadPrimeTable(void);

/* initialize the record hash table and the record lock table, latch table. */

pthread_rwlock_t * RecordLock[TABLENUM];
pthread_spinlock_t * RecordLatch[TABLENUM];
Record* TableList[TABLENUM];

int BucketNum[TABLENUM];
int BucketSize[TABLENUM];
int RecordNum[TABLENUM];

int Prime[150000];
int PrimeNum;

static bool IsUpdateConflict(Record * r, TransactionId tid, uint64_t * tid_array, int count, int min, int max);

/* to see whether the version is a deleted version. */
bool IsMVCCDeleted(Record * r, VersionId v)
{
   if(r->VersionList[v].deleted == true)
      return true;
   else
      return false;
}

/*
 * @return:'true' for visible, 'false' for invisible.
 */
bool MVCCVisible(Record * r, VersionId v, uint64_t * tid_array, int count, int min, int max)
{
	TransactionId tid;

	tid=r->VersionList[v].tid;

	/* if 'tid' is not in snapshot 'snap', then transaction by 'tid' must have committed */
	if(!TidInSnapshot(tid, tid_array, count, min, max))
		return true;
	return false;
}

/* to see whether the transaction can update the data. return true to update, false to abort. */
bool IsUpdateConflict(Record * r, TransactionId tid, uint64_t * tid_array, int count, int min, int max)
{
	/* self already updated the data, note that rear is not the newest version. */
	VersionId newest;

	newest = (r->rear + VERSIONMAX -1) % VERSIONMAX;
	if(r->lcommit != newest)
	{
		assert(r->VersionList[newest].tid == tid);

		/* self already  deleted. */
		if(IsMVCCDeleted(r, newest))
			return false;
		/* self already updated. */
		else
			return true;
	}
	/* self first update the data. */
	else
	{
		/* update permission only when the lcommit version is visible and is not a deleted version. */
		if(MVCCVisible(r, r->lcommit, tid_array, count, min, max) && !IsMVCCDeleted(r, r->lcommit))
			return true;
		else
			return false;
	}
}

/* some functions used for manage the circular queue. */
void InitQueue(Record * r)
{
   int i;
   assert(r != NULL);
   r->tupleid = InvalidTupleId;
   r->rear = 0;
   r->front = 0;
   /* lcommit is means the last version id that commit, its initialized id should be -1 to represent the nothing position */
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

void InitBucketNum_Size(void)
{
	/* bucket num. */
	BucketNum[Warehouse_ID]=1;
    BucketNum[Item_ID]=1;
    BucketNum[Stock_ID]=configWhseCount;
    BucketNum[District_ID]=configWhseCount;
    BucketNum[Customer_ID]=configWhseCount*configDistPerWhse;
    BucketNum[History_ID]=configWhseCount*configDistPerWhse;
    BucketNum[Order_ID]=configWhseCount*configDistPerWhse;
    BucketNum[NewOrder_ID]=configWhseCount*configDistPerWhse;
    BucketNum[OrderLine_ID]=configWhseCount*configDistPerWhse;
    /* bucket size. */
	BucketSize[Warehouse_ID]=configWhseCount;
	BucketSize[Item_ID]=configUniqueItems;
	BucketSize[Stock_ID]=configUniqueItems;
	BucketSize[District_ID]=configDistPerWhse;
	BucketSize[Customer_ID]=configCustPerDist;
	BucketSize[History_ID]=configCustPerDist;
	BucketSize[Order_ID]=OrderMaxNum;
	BucketSize[NewOrder_ID]=OrderMaxNum;
	BucketSize[OrderLine_ID]=OrderMaxNum*10;

	/* adapt the bucket-size to prime. */
	ReadPrimeTable();
	PrimeBucketSize();
}

void InitRecordNum(void)
{
	int i;

	for(i=0;i<TABLENUM;i++)
		RecordNum[i]=BucketNum[i]*BucketSize[i];
}

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

/* initialize the record hash table and the related lock*/
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
	    return (int)((TupleId)(r + (TupleId)k * (1 + (TupleId)(((r >> 5) +1) % (num - 1)))) % num);
    else
	    return 0;
}

int LimitHash(int table_id, TupleId r, int k, int min_max)
{
	int num;
	num=RecordNum[table_id];
	if(min_max-1 > 0)
		return ((r%min_max + k * (1 + (((r>>5) +1) % (min_max - 1)))) % min_max);
	else
		return 0;
}

/* the function RecordFind is used to find a position of a particular tuple id in the HashTable. */
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

int LimitRecordFind(int table_id, TupleId r)
{
   int k = 0;
   int h = 0;
   int w_id, d_id, o_id, bucket_id, min, max, c_id;
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

int RecordFind(int table_id, TupleId r)
{
	return LimitRecordFind(table_id, r);
}

/*
 * the function RecordFind is used to find a position of a particular tuple id in the HashTable for insert.
 *@return:'h' for success, '-2' for already exists, '-1' for not success(already full)
 */
int BasicRecordFindHole(int tableid, TupleId r, int* flag)
{
   int k = 0;
   int h = 0;
   uint64_t num=RecordNum[tableid];

   assert(TableList != NULL);
   THash HashTable = TableList[tableid];
   do
   {
       h = Hash(tableid, r, k);
       /* find a empty record space. */
       if(__sync_bool_compare_and_swap(&HashTable[h].tupleid,InvalidTupleId,r))
       {
    	   /* to make sure that this place by 'h' is empty. */
    	   assert(isEmptyQueue(&HashTable[h]));
    	   *flag=0;
    	   return h;
       }
       /* to compare whether the two tuple_id are equal. */
       else if(HashTable[h].tupleid==r)
       {
  		   printf("the data by %ld is already exist.\n",r);
  		   *flag=1;
  		   return h;
       }
       /* to search the next record place. */
       else
    	   k++;
   } while (k < num);
   printf("can not find a space for insert record %ld %d!\n", r, num);
   *flag=-2;
   return -2;
}

int LimitRecordFindHole(int table_id, TupleId r, int *flag)
{
	int w_id, d_id, o_id, bucket_id, min, max;
	int bucket_size=BucketSize[table_id];
    int k = 0;
    int h = 0;

    int offset=-1;
    TransactionData *tdata;
    bool success;

    assert(TableList != NULL);
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

    do
    {
	    h = min+LimitHash(table_id, r, k, bucket_size);

	    pthread_spin_lock(&RecordLatch[table_id][h]);

	    if(HashTable[h].tupleid == InvalidTupleId)
	    {

	    	if(!isEmptyQueue(&HashTable[h]))
	    	{
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

	    if(success == true)
	    {
		    *flag=0;
		    return h;
	    }
	    /* to compare whether the two tuple_id are equal. */
	    else if(HashTable[h].tupleid==r)
	    {
		    *flag=1;
		    return h;
	    }
	    /* to search the next record place. */
	    else
		   k++;
    } while (k < bucket_size);
    *flag=-2;
    return -2;
}

int RecordFindHole(int table_id, TupleId r, int *flag)
{
	return LimitRecordFindHole(table_id, r, flag);
}

void ProcessInsert(uint64_t * recv_buffer, int conn, int sindex)
{
	int h;
	int status = 1;
	int flag;
	int table_id;
	uint64_t tuple_id;
	table_id = (uint32_t) recv_buffer[1];
    tuple_id = recv_buffer[2];
	h = RecordFindHole(table_id, tuple_id, &flag);

	if(flag==-2)
	{
		/* no space for new tuple to insert. */
		printf("Data_insert: flag==-1.\n");
		printf("no space for table_id:%d, tuple_id:%ld\n",table_id, tuple_id);
		exit(-1);
		status = 0;
	}

	else if(flag==1 && IsInsertDone(table_id, h))
	{
		status = 0;
	}

	if((SSend2(conn, sindex, status, h)) == -1)
		printf("process insert send error!\n");
}

void ProcessTrulyInsert(uint64_t * recv_buffer, int conn, int sindex)
{
	int status = 1;
	int table_id;
	TupleId tuple_id;
	TupleId value;
	uint64_t index;
	TransactionId tid;

    table_id = (uint32_t)recv_buffer[1];
    tuple_id = recv_buffer[2];
    value = recv_buffer[3];
    index = recv_buffer[4];
    tid = (TransactionId)recv_buffer[5];

	pthread_rwlock_wrlock(&(RecordLock[table_id][index]));

	if(IsInsertDone(table_id, index))
	{
		/* other transaction has inserted the tuple. */
		pthread_rwlock_unlock(&(RecordLock[table_id][index]));
		status = 4;
		if((SSend1(conn, sindex, status)) == -1)
			printf("process truly insert send error!\n");
	}
	else
	{
		THash HashTable=TableList[table_id];
		pthread_spin_lock(&RecordLatch[table_id][index]);
		HashTable[index].tupleid=tuple_id;
		EnQueue(&HashTable[index],tid, value);
		pthread_spin_unlock(&RecordLatch[table_id][index]);
		if((SSend1(conn, sindex, status)) == -1)
			printf("process truly insert send error!\n");
	}
}

void ProcessUpdateFind(uint64_t * recv_buffer, int conn, int sindex)
{
   int tableid;
   int h;
   int status = 1;
   uint64_t tupleid;

   tableid = recv_buffer[1];
   tupleid = recv_buffer[2];

   h = RecordFind(tableid, tupleid);
   /* not found. */
   if (h < 0)
   {
	  /* abort transaction outside the function. */
      status = 0;
   }
   if (SSend2(conn, sindex, status, h) == -1)
	   printf("process update find error\n");
}

void ProcessReadFind(uint64_t * recv_buffer, int conn, int sindex)
{
	int tableid;
	int h;
	int status = 1;
	uint64_t tupleid;
	tableid = recv_buffer[1];
	tupleid = recv_buffer[2];
	h = RecordFind(tableid, tupleid);
	/* not found. */
	if (h < 0)
	{
	   /* abort transaction outside the function. */
	   status = 0;
	}
	if (SSend2(conn, sindex, status, h) == -1)
	   printf("process read find error\n");
}

void ProcessReadVersion(uint64_t * recv_buffer, int conn, int sindex)
{
	int h;
	int table_id;
	int i;
	int status = 5;
	int j = 0;
	int count, min, max;
	uint64_t value;

	count = recv_buffer[1];
	min = recv_buffer[2];
	max = recv_buffer[3];

    j = 3 + MAXPROCS;

	table_id = recv_buffer[j+1];
	h = recv_buffer[j+2];
	THash HashTable = TableList[table_id];


	pthread_spin_lock(&RecordLatch[table_id][h]);
	if(HashTable[h].lcommit >= 0)
	{
		for (i = HashTable[h].lcommit; i != (HashTable[h].front + VERSIONMAX - 1) % VERSIONMAX; i = (i + VERSIONMAX - 1) % VERSIONMAX)
		{
			if (MVCCVisible(&(HashTable[h]), i, recv_buffer+4, count, min, max) )
			{
				if(IsMVCCDeleted(&HashTable[h],i))
				{
					pthread_spin_unlock(&RecordLatch[table_id][h]);
					status = 4;
					break;
				}
				else
				{
					pthread_spin_unlock(&RecordLatch[table_id][h]);
					status = 1;
					value = HashTable[h].VersionList[i].value;
					break;
				}
			}
		}
	}

	if (i == (HashTable[h].front + VERSIONMAX - 1) % VERSIONMAX)
    {
		status = 0;
		pthread_spin_unlock(&RecordLatch[table_id][h]);
	}

	if(SSend2(conn, sindex, status, value) == -1)
		printf("process read version send error\n");
}

void ProcessUpdateConflict(uint64_t * recv_buffer, int conn, int sindex)
{
   int count;
   int min;
   int max;
   int j;
   int status = 1;
   int tableid;
   uint64_t h;
   TransactionId tid;
   bool firstadd;

   count = recv_buffer[1];
   min = recv_buffer[2];
   max = recv_buffer[3];

   j = 3 + MAXPROCS;

   tableid = recv_buffer[1+j];
   h = recv_buffer[2+j];
   tid = recv_buffer[3+j];
   firstadd = recv_buffer[4+j];

   THash HashTable=TableList[tableid];
   if (firstadd)
   {
		/* the first time to hold the wr-lock on data (table_id,tuple_id). */
		pthread_rwlock_wrlock(&(RecordLock[tableid][h]));
   }

   if(!IsUpdateConflict(&(HashTable[h]), tid, recv_buffer+4, count, min, max))
   {
	  /* release the write-lock and return to roll back. */
	  if(firstadd)
		 pthread_rwlock_unlock(&(RecordLock[tableid][h]));
	  status = 4;
	}

    if (SSend1(conn, sindex, status) == -1)
   	   printf("process update conflict send error\n");
}

void ProcessUpdateVersion(uint64_t * recv_buffer, int conn, int sindex)
{
   int old,i;
   int tableid;
   uint64_t h;
   TransactionId tid;
   uint64_t value;
   bool isdelete;
   VersionId newest;
   int status = 1;

   tableid = recv_buffer[1];
   h = recv_buffer[2];
   tid = recv_buffer[3];
   value = recv_buffer[4];
   isdelete = recv_buffer[5];

   THash HashTable=TableList[tableid];
   pthread_spin_lock(&RecordLatch[tableid][h]);
   assert(!isEmptyQueue(&HashTable[h]));
   if (!isdelete)
   {
      EnQueue(&HashTable[h], tid, value);
      if (isFullQueue(&(HashTable[h])))
      {
         old = (HashTable[h].front +  VERSIONMAX/3) % VERSIONMAX;
	     for (i = HashTable[h].front; i != old; i = (i+1) % VERSIONMAX)
	     {
	    	HashTable[h].VersionList[i].committime = InvalidTimestamp;
		    HashTable[h].VersionList[i].tid = 0;
		    HashTable[h].VersionList[i].deleted = false;
		    HashTable[h].VersionList[i].value= 0;
	      }
	     HashTable[h].front = old;
      }
      pthread_spin_unlock(&RecordLatch[tableid][h]);
      if (SSend1(conn, sindex, status) == -1)
	     printf("Process update version send error\n");
   }
   else
   {
	   EnQueue(&HashTable[h], tid, 0);
	   newest = (HashTable[h].rear + VERSIONMAX -1) % VERSIONMAX;
	   HashTable[h].VersionList[newest].deleted = true;
	   if (isFullQueue(&(HashTable[h])))
	   {
	      old = (HashTable[h].front +  VERSIONMAX/3) % VERSIONMAX;
		  for (i = HashTable[h].front; i != old; i = (i+1) % VERSIONMAX)
		  {
			 HashTable[h].VersionList[i].committime = InvalidTimestamp;
			 HashTable[h].VersionList[i].tid = 0;
			 HashTable[h].VersionList[i].deleted = false;

			 HashTable[h].VersionList[i].value= 0;
		   }
		   HashTable[h].front = old;
	   }
	   pthread_spin_unlock(&RecordLatch[tableid][h]);
	   if (SSend1(conn, sindex, status) == -1)
		  printf("Process update version send error\n");
   }
}

void ProcessCommitInsert(uint64_t * recv_buffer, int conn, int sindex)
{
	int table_id;
	int status = 0;
	uint64_t index;
	TimeStampTz ctime;

	table_id = recv_buffer[1];
	index = recv_buffer[2];
	ctime = recv_buffer[3];

	THash HashTable=TableList[table_id];
	Record * r = &(HashTable[index]);

    pthread_spin_lock(&RecordLatch[table_id][index]);
	r->lcommit = (r->lcommit + 1) % VERSIONMAX;
	r->VersionList[r->lcommit].committime = 0;
	pthread_spin_unlock(&RecordLatch[table_id][index]);

	if((SSend1(conn, sindex, status)) == -1)
		printf("process commit insert send error!\n");
}

void ProcessCommitUpdate(uint64_t * recv_buffer, int conn, int sindex)
{
   int tableid;
   uint64_t h;
   TimeStampTz ctime;
   int status = 0;

   tableid = recv_buffer[1];
   h = recv_buffer[2];
   ctime = recv_buffer[3];

   THash HashTable=TableList[tableid];
   Record *r = &HashTable[h];

   pthread_spin_lock(&RecordLatch[tableid][h]);
   r->lcommit = (r->lcommit + 1) % VERSIONMAX;
   r->VersionList[r->lcommit].committime = 0;
   pthread_spin_unlock(&RecordLatch[tableid][h]);

   if (SSend1(conn, sindex, status) == -1)
	   printf("commit update send error\n");
}

void ProcessAbortInsert(uint64_t * recv_buffer, int conn, int sindex)
{
   int tableid;
   uint64_t h;
   int status = 0;
   VersionId newest;

   tableid = recv_buffer[1];
   h = recv_buffer[2];

   THash HashTable=TableList[tableid];
   Record *r = &HashTable[h];

   pthread_spin_lock(&RecordLatch[tableid][h]);
   newest = (r->rear + VERSIONMAX -1) % VERSIONMAX;
   r->tupleid=InvalidTupleId;
   r->rear=0;
   r->front=0;
   r->lcommit=-1;
   r->VersionList[newest].tid = InvalidTransactionId;
   r->VersionList[newest].value = 0;
   pthread_spin_unlock(&RecordLatch[tableid][h]);

   if (SSend1(conn, sindex, status) == -1)
      printf("abort insert send error\n");
}

void ProcessAbortUpdate(uint64_t * recv_buffer, int conn, int sindex)
{
   int tableid;
   uint64_t h;
   bool isdelete;
   int status = 0;
   VersionId newest;

   tableid = recv_buffer[1];
   h = recv_buffer[2];
   isdelete = recv_buffer[3];

   THash HashTable=TableList[tableid];
   Record *r = &HashTable[h];
   if (isdelete)
   {
      pthread_spin_lock(&RecordLatch[tableid][h]);
      newest = (r->rear + VERSIONMAX -1) % VERSIONMAX;
      r->VersionList[newest].tid = InvalidTransactionId;
      r->VersionList[newest].deleted = false;
      r->rear = newest;
      pthread_spin_unlock(&RecordLatch[tableid][h]);
      if (SSend1(conn, sindex, status) == -1)
   	     printf("abort update send error\n");
   }
   else
   {
	  pthread_spin_lock(&RecordLatch[tableid][h]);
	  newest = (r->rear + VERSIONMAX -1) % VERSIONMAX;
	  r->VersionList[newest].tid = InvalidTransactionId;
      r->VersionList[newest].value = 0;
	  r->rear = newest;
	  pthread_spin_unlock(&RecordLatch[tableid][h]);
	  if (SSend1(conn, sindex, status) == -1)
		  printf("abort update send error\n");
   }
}

void ProcessUnrwLock(uint64_t * recv_buffer, int conn, int sindex)
{
   uint32_t table_id;
   uint64_t index;
   uint64_t status = 1;

   table_id = recv_buffer[1];
   index = recv_buffer[2];

   pthread_rwlock_unlock(&(RecordLock[table_id][index]));
   if ((SSend1(conn, sindex, status)) == -1)
      printf("lock process send error\n");
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
	printf("table: %d of %d rows are available.\n",count, RecordNum[table_id]);
}

/*
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
