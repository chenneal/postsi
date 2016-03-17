/*
 * data.h
 *
 *  Created on: Jan 29, 2016
 *      Author: Yu
 */

#ifndef DATA_H_
#define DATA_H_
#include<stdbool.h>
#include"type.h"
#include"timestamp.h"

#define TABLENUM  9
#define VERSIONMAX 20

#define InvalidTupleId (TupleId)(0)

/* add by yu the data structure for the tuple */

/* Version is used for store a version of a record */
typedef struct {
	TransactionId tid;
	TimeStampTz committime;
	bool deleted;
	/* to stock other information of each version. */
	TupleId value;
} Version;

/*  Record is a multi-version tuple structure */
typedef struct {
	TupleId tupleid;
	int rear;
	int front;
	int lcommit;
	Version VersionList[VERSIONMAX];
} Record;

/* THash is pointer to a hash table for every table */
typedef Record * THash;

typedef int VersionId;

/* the lock in the tuple is used to verify the atomic operation of transaction */
extern pthread_rwlock_t* RecordLock[TABLENUM];

/* just use to verify the atomic operation of a short-time */
extern pthread_spinlock_t* RecordLatch[TABLENUM];

/* every table will have a separated HashTable */
extern Record* TableList[TABLENUM];

extern int BucketNum[TABLENUM];
extern int BucketSize[TABLENUM];

extern int RecordNum[TABLENUM];;

extern bool MVCCVisible(Record * r, VersionId v, uint64_t * tid_array, int count, int min, int max);
extern void ProcessInsert(uint64_t * recv_buffer, int conn, int sindex);
extern void ProcessTrulyInsert(uint64_t * recv_buffer, int conn, int sindex);
extern void ProcessUpdateFind(uint64_t * recv_buffer, int conn, int sindex);
extern void ProcessReadFind(uint64_t * recv_buffer, int conn, int sindex);
extern void ProcessReadVersion(uint64_t * recv_buffer, int conn, int sindex);
extern void ProcessUpdateConflict(uint64_t * recv_buffer, int conn, int sindex);
extern void ProcessUpdateVersion(uint64_t * recv_buffer, int conn, int sindex);
extern void ProcessCommitInsert(uint64_t * recv_buffer, int conn, int sindex);
extern void ProcessCommitUpdate(uint64_t * recv_buffer, int conn, int sindex);
extern void ProcessAbortInsert(uint64_t * recv_buffer, int conn, int sindex);
extern void ProcessAbortUpdate(uint64_t * recv_buffer, int conn, int sindex);
extern void ProcessUnrwLock(uint64_t * recv_buffer, int conn, int sindex);

#endif
