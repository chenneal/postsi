/*
 * data_record.h
 *
 *  Created on: Nov 23, 2015
 *      Author: xiaoxin
 */

#ifndef DATA_RECORD_H_
#define DATA_RECORD_H_

#include "type.h"

#define DataNumSize sizeof(int)

/*
 * type definitions for data update.
 */
typedef enum UpdateType
{
	//data insert
	DataInsert,
	//data update
	DataUpdate,
	//data delete
	DataDelete
}UpdateType;

struct DataRecord
{
	UpdateType type;
	//data pointer.
	//void* data;

	int table_id;
	TupleId tuple_id;

	//other information attributes.
	TupleId value;

	//index in the table.
	int index;
};
typedef struct DataRecord DataRecord;

extern void InitDataMemAlloc(void);

extern void InitDataMem(void);

//extern void InsertRecord(void* data);

//extern void UpdateRecord(void* olddata,void* newdata);

//extern void DeleteRecord(void* data);

extern void DataRecordInsert(DataRecord* datard);

extern Size DataMemSize(void);

extern void CommitDataRecord(TransactionId tid, CommitId cid);

extern void AbortDataRecord(TransactionId tid, int trulynum);

extern void DataRecordSort(DataRecord* dr, int num);

extern TupleId IsDataRecordVisible(char* DataMemStart, int table_id, TupleId tuple_id);

#endif /* DATA_RECORD_H_ */
