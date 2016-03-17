/*
 * data_am.h
 *
 *  Created on: Nov 26, 2015
 *      Author: xiaoxin
 */

#ifndef DATA_AM_H_
#define DATA_AM_H_

#include"type.h"

extern int Data_Insert(int table_id, TupleId tuple_id, TupleId value, int nid);

extern int Data_Update(int table_id, TupleId tuple_id, TupleId value, int nid);

extern int Data_Delete(int table_id, TupleId tuple_id, int nid);

extern TupleId Data_Read(int table_id, TupleId tuple_id, int nid, int* flag);

extern int TrulyDataInsert(int table_id, uint64_t index, TupleId tuple_id, TupleId value, int nid);

extern int TrulyDataUpdate(int table_id, uint64_t index, TupleId tuple_id, TupleId value, int nid);

extern int TrulyDataDelete(int table_id, uint64_t index, TupleId tuple_id, int nid);

extern int Light_Data_Read(int table_id, int h);

extern void PrintTable(int tableid);

extern void validation(int table_id);

extern void WriteCollusion(TransactionId tid, int index);

#endif /* DATA_AM_H_ */
