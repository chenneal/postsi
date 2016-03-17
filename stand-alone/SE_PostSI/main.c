/*
 * main.c
 *
 *  Created on: Nov 10, 2015
 *      Author: xiaoxin
 */
#include <stdio.h>
#include <pthread.h>
#include <malloc.h>
#include "mem.h"
#include "thread_main.h"
#include "data_am.h"
#include "transactions.h"
#include "config.h"

int main(void)
{
	int i;
	int numTerminals=16;

	InitSys();

	printf("InitSys finished.\n");

	dataLoading();
	//PrintTable(Warehouse_ID);

	RunTerminals(NumTerminals);
	PrintTable(Item_ID);
	for(i=0;i<TABLENUM;i++)
	{
		//PrintTable(i);
	}

	ExitSys();

	printf("finished.\n");
	return 0;
}

