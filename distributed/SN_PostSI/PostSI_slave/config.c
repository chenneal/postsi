/*
 * config.c
 *
 *  Created on: Dec 23, 2015
 *      Author: xiaoxin
 */

#include "config.h"
#include "transactions.h"

int configWhseCount;

int configDistPerWhse;

int configCustPerDist;

int configUniqueItems;

int MaxBucketSize;

int configCommitCount;

int transactionsPerTerminal;
int paymentWeightValue;
int orderStatusWeightValue;
int deliveryWeightValue;
int stockLevelWeightValue;
int limPerMin_Terminal;


void InitConfig(void)
{
   transactionsPerTerminal=2000;
   paymentWeightValue=43;
   orderStatusWeightValue=0;
   deliveryWeightValue=0;
   stockLevelWeightValue=4;
   limPerMin_Terminal=0;

   configWhseCount=2;
   configDistPerWhse=10;
   configCustPerDist=3000;
   MaxBucketSize=1000000;
   configUniqueItems=100000;

   configCommitCount=60;
}
