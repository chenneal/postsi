/*
 * data_am.h
 *
 *  Created on: Dec 7, 2015
 *      Author: xiaoxin
 */

#ifndef DATA_AM_H_
#define DATA_AM_H_

#include <stdbool.h>
#include "config.h"
#include "type.h"
#include "timestamp.h"
#include "snapshot.h"

#define InvalidTupleId (TupleId)(0)


//structure for version of one tuple.
typedef struct {
	TransactionId tid;
	TimeStampTz committime;
	bool deleted;

	//to store other information of each version.
	TupleId value;
} Version;

//structure for tuple.
typedef struct {
	TupleId tupleid;
	int rear;
	int front;
	int lcommit;
	Version VersionList[VERSIONMAX];
} Record;

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

extern int RecordNum[TABLENUM];;

extern int Data_Insert(int table_id, TupleId tuple_id, TupleId value);

extern int Data_Update(int table_id, TupleId tuple_id, TupleId value);

extern int Data_Delete(int table_id, TupleId tuple_id);

extern TupleId Data_Read(int table_id, TupleId tuple_id, int *flag);

extern TupleId LocateData_Read(int table_id, int h, TupleId *id);

extern void InitRecord(void);

extern bool MVCCVisible(Record * r, VersionId v, Snapshot* snap);

extern int TrulyDataInsert(void* data, int table_id, int index, TupleId tuple_id, TupleId value);

extern int TrulyDataUpdate(void* data, int table_id, int index, TupleId tuple_id, TupleId value);

extern int TrulyDataDelete(void* data, int table_id, int index, TupleId tuple_id);

extern void PrintTable(int table_id);

#endif /* DATA_AM_H_ */
