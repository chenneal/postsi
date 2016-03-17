/*
 * snapshot.h
 *
 *  Created on: Dec 7, 2015
 *      Author: xiaoxin
 */

#ifndef SNAPSHOT_H_
#define SNAPSHOT_H_

#include "type.h"

struct SnapshotData
{
	TransactionId tid_min;
	TransactionId tid_max;

	int tcount;

	TransactionId* tid_array;
};

typedef struct SnapshotData Snapshot;

extern void InitTransactionSnapshotDataMemAlloc(void);

extern void InitTransactionSnapshotData(void);

extern void GetTransactionSnapshot(void);

extern bool TidInSnapshot(TransactionId tid, Snapshot* snap);

extern Size SnapshotSize(void);

#endif /* SNAPSHOT_H_ */
