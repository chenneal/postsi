/*
 * main.c
 *
 *  Created on: Dec 7, 2015
 *      Author: xiaoxin
 */
#include <stdio.h>

#include "mem.h"
#include "thread_main.h"
#include "data_am.h"
#include "transactions.h"
#include "config.h"
#include "proc.h"

int main(void)
{
	InitSys();

	dataLoading();

	RunTerminals(NumTerminals);

	ExitSys();

	printf("finished.\n");
	return 0;
}

