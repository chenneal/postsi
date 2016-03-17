/*
 * lock_record.h
 *
 *  Created on: Nov 23, 2015
 *      Author: xiaoxin
 */

#ifndef LOCK_RECORD_H_
#define LOCK_RECORD_H_

#include "config.h"
#include "type.h"

typedef enum LockMode
{
	LOCK_SHARED,
	LOCK_EXCLUSIVE
}LockMode;

struct DataLock
{
	uint32_t table_id;
	TupleId tuple_id;
	LockMode lockmode;
	//lock pointer or data pointer.
	void* ptr;
};

typedef struct DataLock DataLock;

extern void InitDataLockMemAlloc(void);

extern void InitDataLockMem(void);

extern int DataLockInsert(DataLock* lock);

extern void DataLockRelease(void);

extern int IsWrLockHolding(uint32_t table_id, TupleId tuple_id);

extern int IsRdLockHolding(uint32_t table_id, TupleId tuple_id);

extern int IsDataLockExist(int table_id, TupleId tuple_id, LockMode mode);
#endif /* LOCK_RECORD_H_ */
