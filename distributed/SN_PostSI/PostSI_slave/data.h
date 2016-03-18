#ifndef DATA_H_
#define DATA_H_
/* add by yu the data structure for the tuple */
#include <stdbool.h>
#include "type.h"
#define RECORDNUM 100000
#define TABLENUM  9
#define VERSIONMAX 20

#define InvalidTupleId (TupleId)(0)


// Version is used for store a version of a record
typedef struct {
	TransactionId tid;
	CommitId cid;

	//to stock other information of each version.
	TupleId value;

	bool deleted;
} Version;

// Record is a multi-version tuple structure
typedef struct {
	TupleId tupleid;
	int rear;
	int front;
	int lcommit;
	Version VersionList[VERSIONMAX];
} Record;

// THash is pointer to a hash table for every table
typedef Record * THash;

typedef int VersionId;

// the lock in the tuple is used to verify the atomic operation of transaction
extern pthread_rwlock_t* RecordLock[TABLENUM];

// just use to verify the atomic operation of a short-time
extern pthread_spinlock_t* RecordLatch[TABLENUM];

// every table will have a separated HashTable
extern Record* TableList[TABLENUM];

extern int BucketNum[TABLENUM];
extern int BucketSize[TABLENUM];

extern uint64_t RecordNum[TABLENUM];

extern void InitRecord(void);
//'tuple_id' is the key attributes, and 'value' is other attributes.
extern void ProcessInsert(uint64_t * recv_buffer, int conn, int index);
extern void ProcessTrulyInsert(uint64_t * recv_buffer, int conn, int index);
extern void ProcessCommitInsert(uint64_t * recv_buffer, int conn, int index);
extern void ProcessUpdate(uint64_t * recv_buffer, int conn, int index);
extern void ProcessRead(uint64_t * recv_buffer, int conn, int index);
extern void ProcessUnrwLock(uint64_t * recv_buffer, int conn, int index);
extern void ProcessUnspinLock(uint64_t * recv_buffer, int conn, int index);
extern void ProcessReadFind(uint64_t * recv_buffer, int conn, int index);
extern void ProcessCollisionInsert(uint64_t * recv_buffer, int conn, int index);
extern void ProcessReadVersion(uint64_t * recv_buffer, int conn, int index);
extern void ProcessUpdateFind(uint64_t * recv_buffer, int conn, int index);
extern void ProcessUpdateWirteList(uint64_t * recv_buffer, int conn, int index);
extern void ProcessUpdateConflict(uint64_t * recv_buffer, int conn, int index);
extern void ProcessUpdateVersion(uint64_t * recv_buffer, int conn, int index);
extern void ProcessGetSidMin(uint64_t * recv_buffer, int conn, int index);
extern void ProcessUpdateStartId(uint64_t * recv_buffer, int conn, int index);
extern void ProcessUpdateCommitId(uint64_t * recv_buffer, int conn, int index);
extern void ProcessCommitUpdate(uint64_t * recv_buffer, int conn, int index);
extern void ProcessAbortUpdate(uint64_t * recv_buffer, int conn, int index);
extern void ProcessAbortInsert(uint64_t * recv_buffer, int conn, int index);
extern void ProcessRestPair(uint64_t * recv_buffer, int conn, int index);

#endif
