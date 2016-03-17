/*
 * trans_conflict.h
 *
 *  Created on: Nov 12, 2015
 *      Author: xiaoxin
 */

/*
 * structures of conflict transactions.
 */

#ifndef TRANS_CONFLICT_H_
#define TRANS_CONFLICT_H_
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include "type.h"

#define ConfTrue 1
#define ConfFalse 0

typedef int TransConf;

extern void InitInvisibleTable(void);

extern void InvisibleTableInsert(int row_index,int column_index);

extern void InvisibleTableReset(int row,int column);

extern CommitId GetTransactionCid(int index,CommitId cid_min);

extern int CommitInvisibleUpdate(int index, StartId sid, CommitId cid);

extern void AtEnd_InvisibleTable(int index);

#endif /* TRANS_CONFLICT_H_ */
