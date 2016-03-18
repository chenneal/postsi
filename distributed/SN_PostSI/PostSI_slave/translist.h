/*
 * translist.h
 *
 *  Created on: Dec 1, 2015
 *      Author: xiaoxin
 */

#ifndef TRANSLIST_H_
#define TRANSLIST_H_

#include"data.h"
#include"proc.h"

#define READLISTMAX 150
#define READLISTTABLEMAX RECORDNUM   //define a read list just a number as a record number for convenience
#define WRITELISTTABLEMAX RECORDNUM  //the same reason as before for read list

typedef struct WriteTransListNode {
	TransactionId transactionid;
	int index;
} WriteTransListNode;

//extern TransactionId ReadTransTable[TABLENUM][READLISTTABLEMAX][READLISTMAX];
//extern WriteTransListNode WriteTransTable[TABLENUM][WRITELISTTABLEMAX];
extern TransactionId* ReadTransTable[TABLENUM][READLISTMAX];
extern TransactionId* WriteTransTable[TABLENUM];

//ReadTransLock is to lock the ReadTransList, and the same as the WriteTransLock
//extern pthread_spinlock_t ReadTransLock[TABLENUM][RECORDNUM];
//extern pthread_spinlock_t WriteTransLock[TABLENUM][RECORDNUM];
extern void ReadListInsert(int tableid, int h, TransactionId tid, int index);
extern TransactionId ReadListRead(int tableid, int h, int test);
extern void ReadListDelete(int tableid, int h, int index);
extern void WriteListInsert(int tableid, int h, TransactionId tid);
extern TransactionId WriteListRead(int tableid, int h);
extern int WriteListReadindex(int tableid, int h);
extern void WriteListDelete(int tableid, int h);

extern void InitTransactionList(void);

extern void InitReadListMemAlloc(void);

extern void InitReadListMem(void);

extern void MergeReadList(uint64_t* buffer);

#endif /* TRANSLIST_H_ */
