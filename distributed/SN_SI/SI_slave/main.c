/*
 * main.c
 *
 *  Created on: Dec 7, 2015
 *      Author: xiaoxin
 */
#include <stdio.h>
#include <pthread.h>
#include <unistd.h>
#include "mem.h"
#include "thread_main.h"
#include "data_am.h"
#include "transactions.h"
#include "config.h"
#include "proc.h"
#include "socket.h"

int main(int argc, char *argv[])
{
	pid_t pid;

	/* do some ready work before start the distributed system */
	GetReady();

	if ((pid = fork()) < 0)
	{
	   printf("fork error\n");
	}

	else if(pid == 0)
	{
		int i;
		/* storage process */
		InitStorage();
		ExitSys();
		printf("storage process finished.\n");
	}

	else
	{
	   /* transaction process */
	   InitTransaction();
       dataLoading();
       /* wait other slave nodes until all of them have loaded data. */
       WaitDataReady();
       /* begin run the benchmark. */
       RunTerminals(THREADNUM);

	   printf("transaction process finished.\n");
	}
	return 0;
}

