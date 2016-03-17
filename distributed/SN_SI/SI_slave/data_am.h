/*
 * data_am.h
 *
 *  Created on: Dec 7, 2015
 *      Author: xiaoxin
 */

#ifndef DATA_AM_H_
#define DATA_AM_H_

#include<stdbool.h>
#include"type.h"
#include"timestamp.h"
#include"snapshot.h"
#include"data.h"

extern int Data_Insert(int table_id, TupleId tuple_id, TupleId value, int nid);

extern int Data_Update(int table_id, TupleId tuple_id, TupleId value, int nid);

extern int Data_Delete(int table_id, TupleId tuple_id, int nid);

extern TupleId Data_Read(int table_id, TupleId tuple_id, int nid, int *flag);

extern TupleId LocateData_Read(int table_id, int h, TupleId *id);

extern void InitRecord(void);

extern int TrulyDataInsert(int table_id, int index, TupleId tuple_id, TupleId value, int nid);

extern int TrulyDataUpdate(int table_id, int index, TupleId tuple_id, TupleId value, int nid);

extern int TrulyDataDelete(int table_id, int index, TupleId tuple_id, int nid);

extern void PrintTable(int table_id);

extern void validation(int table_id);

#endif /* DATA_AM_H_ */
