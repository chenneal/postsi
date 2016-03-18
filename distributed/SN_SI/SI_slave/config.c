/*
 * config.c
 *
 *  Created on: Jan 5, 2016
 *      Author: xiaoxin
 */

#include "config.h"
#include "transactions.h"

// number of warehouses
int configWhseCount;

// number of districts per warehouse
int configDistPerWhse;

// number of customers per district
int configCustPerDist;

// number of items
int configUniqueItems;

int MaxBucketSize;

// max number of tuples operated in one transaction
int configCommitCount;

// number of transactions per terminal
int transactionsPerTerminal;

// ratio of each transaction in one terminal
int paymentWeightValue;
int orderStatusWeightValue;
int deliveryWeightValue;
int stockLevelWeightValue;

int limPerMin_Terminal;

//make sure that 'NumTerminals' <= 'MAXPROCS'.
int NumTerminals;

//the limited max number of new orders for each district.
int OrderMaxNum;

//the max number of wr-locks held in one transaction.
int MaxDataLockNum;

void InitConfig(void)
{
	//transPerTerminal
	transactionsPerTerminal=2000;

	//we didn't build index on tables, so range query in order-status and delivery transactions are very slow,
	//there we set 'orderStatusWeightValue' and 'deliveryWeightValue' to '0', so we actually didn't implement
	//those two transactions order-status transaction and delivery transaction.
	paymentWeightValue=43;
	orderStatusWeightValue=0;
	deliveryWeightValue=0;
	stockLevelWeightValue=4;

	limPerMin_Terminal=0;

	configWhseCount=10;
	configDistPerWhse=10;
	configCustPerDist=3000;
	MaxBucketSize=1000000;
	configUniqueItems=100000;

	configCommitCount=60;

	OrderMaxNum=4500;

	MaxDataLockNum=80;
}
