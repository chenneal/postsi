/*
 * translist.h
 *
 *  Created on: Dec 1, 2015
 *      Author: xiaoxin
 */

#ifndef TRANSLIST_H_
#define TRANSLIST_H_

#include "config.h"
#include "data_am.h"
#include "proc.h"
#include "timestamp.h"

typedef struct WriteTransListNode {
	TransactionId transactionid;
	int index;
} WriteTransListNode;


extern TransactionId* ReadTransTable[TABLENUM][READLISTMAX];

extern TransactionId* WriteTransTable[TABLENUM];

extern void ReadListInsert(int tableid, int h, TransactionId tid, int index);
extern TransactionId ReadListRead(int tableid, int h, int test);
extern void ReadListDelete(int tableid, int h, int index);
extern void WriteListInsert(int tableid, int h, TransactionId tid);
extern TransactionId WriteListRead(int tableid, int h);
extern void WriteListDelete(int tableid, int h, int index);

extern void WriteCollusion(TransactionId tid, int index);

extern void InitTransactionList(void);

extern void InitReadListMemAlloc(void);

extern void InitReadListMem(void);

extern void MergeReadList(int tableid, int h);

extern int ReadCollusion(int tableid, int h, TransactionId tid, int index);;

#endif /* TRANSLIST_H_ */
