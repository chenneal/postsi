/*
 * trans_conflict.c
 *
 *  Created on: Nov 12, 2015
 *      Author: xiaoxin
 */

/*
 *interface to deal with conflict transactions.
 */
#include <malloc.h>
#include <stdlib.h>
#include <assert.h>

#include "trans_conflict.h"
#include "proc.h"
#include "trans.h"
#include "lock.h"
#include "config.h"

//pointer to global conflict transactions table.
TransConf* TransConfTable[MAXPROCS];

void InvisibleTableInsert(int row_index,int column_index);

void InvisibleTableReset(int row,int column);

CommitId GetTransactionCid(int index,CommitId cid_min);

int CommitInvisibleUpdate(int index, StartId sid, CommitId cid);

void AtEnd_InvisibleTable(int index);

inline bool IsPairInvisible(int row, int column);
TransConf* InvisibleTableLocate(int row, int column);

Size InvisibleTableSize(void) {
	Size size;
	size = MAXPROCS * MAXPROCS * sizeof(TransConf);
	return size;
}

/*
 * function: memory allocation for conflict transactions table.
 */
void InitInvisibleTable(void) {
	int i;
	Size size;

	size=MAXPROCS * sizeof(TransConf);

	for(i=0;i<MAXPROCS;i++)
	{
		TransConfTable[i]=(TransConf*)malloc(size);
		if(TransConfTable[i] == NULL)
		{
			printf("malloc error for transaction conflict table.\n");
			exit(-1);
		}
		memset((char*)TransConfTable[i], ConfFalse, size);
	}
}

/*
 * (row,column)'s offset in conflict transactions table.
 */
Size InvisibleTableOffset(int row, int column) {
	Size offset;

	offset = (row * MAXPROCS + column) * sizeof(TransConf);

	return offset;
}

/*
 * function: insert a conflict transaction tuple into the table.
 * @input: 'IsRead': 'true' means the invisible tuple is inserted by transaction's read,
 * 'false' means the invisible tuple is inserted by transaction's update.
 */
void InvisibleTableInsert(int row_index, int column_index)
{
	TransConfTable[row_index][column_index]=ConfTrue;
}

/*
 * function: reset the (row,column)'s conflict transaction tuple to invalid tuple.
 */
void InvisibleTableReset(int row, int column)
{
	TransConfTable[row][column]=ConfFalse;
}

/*
 * return a pointer in the conflict transactions table.
 */
TransConf* InvisibleTableLocate(int row, int column) {
	assert(row<MAXPROCS && column<MAXPROCS);

	TransConf* transconf;

	int offset;

	offset=row * MAXPROCS + column;

	transconf=(TransConf*)TransConfTable+offset;

	return transconf;
}

/*
 * @return:'true' for invisible, 'false' for visible.
 */
bool IsPairInvisible(int row, int column)
{
	bool visible;

	visible=(TransConfTable[row][column] == 1)? true:false;

	return visible;
}

/*
 * make sure the final cid of the transaction.
 */
CommitId GetTransactionCid(int index,CommitId cid_min)
{
	int i;
	CommitId cid;
	StartId sid;
	cid=cid_min;


	for(i=0;i<=NumTerminals;i++)
	{
		if(TransConfTable[index][i] && i!=index)
		{
			sid=GetTransactionSid(i);
			if(sid >= 0)
				cid=(cid<sid)?sid:cid;
		}
	}

	return cid;
}

/*
 * Is transaction by 'cid' to rollback because of conflict
 * with other transaction by invisible pair.
 * @return:'1':rollback,'0':not rollback.
 */
int IsConflictRollback(int index,CommitId cid)
{
	int i;
	int conflict=0;
	for(i=0;i<MAXPROCS;i++)
	{
		if(IsPairInvisible(index,i) && IsPairConflict(i,cid) && index != i)
		{
			conflict=1;
			break;
		}
	}
	return conflict;
}

/*
 * update the conflict transactions' sid and cid once committing.
 * @return:'1' to rollback, '0' to commit
 */
int CommitInvisibleUpdate(int index,StartId sid, CommitId cid)
{
	//return 0;
	int i;
	int result;

	//current transaction can succeed in committing, so update
	//other transaction by invisible transaction pair.
	for(i=0;i<=NumTerminals;i++)
	{
		if(i != index)
		{
			if(TransConfTable[index][i])
			{
				result=UpdateProcStartId(i,cid);
				if(result==0)
					return 1;
			}
		}
	}

	for(i=0; i<=NumTerminals;i++)
	{
		if(i != index)
		{
			if(TransConfTable[i][index])
			{
				result=UpdateProcCommitId(i,sid);
				if(result==0)
					return 2;
			}
		}
	}
	return 0;
}

/*
 * clean the invisible table at the end of transaction by index;
 */
void AtEnd_InvisibleTable(int index)
{
	int i;
	for(i=0;i<= NumTerminals;i++)
	{
		TransConfTable[index][i]=ConfFalse;
		TransConfTable[i][index]=ConfFalse;
	}
}
