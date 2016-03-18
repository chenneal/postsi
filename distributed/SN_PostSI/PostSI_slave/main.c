/*
 * main.c
 *
 *  Created on: Nov 10, 2015
 *      Author: xiaoxin
 */

#include<stdio.h>
#include<unistd.h>
#include<sys/shm.h>
#include<pthread.h>
#include<malloc.h>
#include"mem.h"
#include"thread_main.h"
#include"data_am.h"
#include"config.h"
#include"socket.h"

int main(int argc, char *argv[])
{
	pid_t pid;

	GetReady();

    if ((pid = fork()) < 0)
    {
    	printf("fork error\n");
    }

    else if(pid == 0)
    {
    	BindShmem();

    	InitStorage();

    	printf("storage process finished.\n");
    }

    else
    {
    	InitTransaction();
    	dataLoading();
    	WaitDataReady();
    	RunTerminals(THREADNUM);
    	printf("transaction process finished.\n");
    }
	return 0;
}

