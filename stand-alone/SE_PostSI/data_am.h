/*
 * data_am.h
 *
 *  Created on: Nov 26, 2015
 *      Author: xiaoxin
 */

#ifndef DATA_AM_H_
#define DATA_AM_H_

#include <stdbool.h>
#include "config.h"
#include "type.h"

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

//'tuple_id' is the key attributes, and 'value' is other attributes.

extern int Data_Insert(int table_id, TupleId tuple_id, TupleId value);

extern int Data_Update(int table_id, TupleId tuple_id, TupleId value);

extern int Data_Delete(int table_id, TupleId tuple_id);

extern TupleId Data_Read(int table_id, TupleId tuple_id, int* flag);

extern int Light_Data_Read(int table_id, int h);

extern TupleId LocateData_Read(int table_id, int h, TupleId *id, bool* abort);

extern void InitRecord(void);

extern int TrulyDataInsert(int table_id, int index, TupleId tuple_id, TupleId value);

extern int TrulyDataUpdate(int table_id, int index, TupleId tuple_id, TupleId value);

extern int TrulyDataDelete(int table_id, int index, TupleId tuple_id);

extern void PrintTable(int table_id);

extern void validation(int table_id);

#endif /* DATA_AM_H_ */
