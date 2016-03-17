/*
 * threadmain.c
 *
 *  Created on: Nov 11, 2015
 *      Author: xiaoxin
 */
#include <sched.h>
#include <pthread.h>
#include <stdlib.h>
#include <time.h>
#include <sys/time.h>

#include "config.h"
#include "proc.h"
#include "mem.h"
#include "trans.h"
#include "thread_global.h"
#include "thread_main.h"
#include "util.h"
#include "data_record.h"
#include "lock_record.h"
#include "lock.h"
#include "data_am.h"

pthread_barrier_t barrier;

TimeStampTz sessionStartTimestamp;

TimeStampTz sessionEndTimestamp;

uint64_t fastNewOrderCounter=0;

uint64_t transactionCount=0;

uint64_t transactionCommit=0;

uint64_t transactionAbort=0;

double tpmC, tpmTotal;

int SleepTime;

struct tm sessionStart, sessionEnd;

void EndReport(TransState* StateInfo, int terminals);

void InitSys(void)
{
	SleepTime=1;

	InitConfig();

	SetRandomSeed();

	//initialize the memory space to be used.
	InitMem();

	//initialize the process array and it's head.
	InitProc();

	//initialize the assignment of global transaction ID.
	InitTransactionIdAssign();

	//initialize pthread_key_t array for thread global variables.
	InitThreadGlobalKey();

	//initialize the global lock.
	InitLock();

	InitRecord();
}

/*
 * function: run threads (terminals).
 */
void ThreadRun(int nthreads)
{
	int i,err;
	cpu_set_t set;

	pthread_t tid[nthreads];

	/*
	 * run threads here.
	 */
	for(i=0;i<nthreads;i++)
	{
		//bind each thread to CPU here.
		err=pthread_create(&tid[i],NULL,ProcStart,NULL);
		if(err!=0)
		{
			printf("can't create thread:%s\n",strerror(err));
			return;
		}
	}

	for(i=0;i<nthreads;i++)
	{
		pthread_join(tid[i],NULL);
	}

	return;
}

void ExitSys(void)
{
	int i;

	FreeMem();

	for(i=0;i<TABLENUM;i++)
	{
		free(TableList[i]);
	}
}

/*
 * function: run terminals specially for TPCC benhmark.
 */
void RunTerminals(int numTerminals)
{
	int i, j;
	int terminalWarehouseID, terminalDistrictID;

	int usedTerminal[configWhseCount][10];
	pthread_t tid[numTerminals];

	TransState* StateInfo=(TransState*)malloc(sizeof(TransState)*numTerminals);

	i=pthread_barrier_init(&barrier, NULL, numTerminals);
	if(i != 0)
	{
		printf("[ERROR]Coundn't create the barrier\n");
		exit(-1);
	}

	int cycle;

	for(i=0;i<configWhseCount;i++)
		for(j=0;j<10;j++)
			usedTerminal[i][j]=0;

	sessionStartTimestamp=GetCurrentTimestamp();

	printf("run terminals..\n");
	for(i=0;i<numTerminals;i++)
	{
		cycle=0;
		do
		{
			terminalWarehouseID=(int)GlobalRandomNumber(1, configWhseCount);
			terminalDistrictID=(int)GlobalRandomNumber(1, 10);
			cycle++;
		}while(usedTerminal[terminalWarehouseID-1][terminalDistrictID-1]);
		usedTerminal[terminalWarehouseID-1][terminalDistrictID-1]=1;

		printf("terminal %d is running, w_id=%d, d_id=%d\n",i,terminalWarehouseID,terminalDistrictID);

		runTerminal(terminalWarehouseID, terminalDistrictID, &tid[i], &barrier, &StateInfo[i]);
		sleep(SleepTime);
	}

	for(i=0;i<numTerminals;i++)
	{
		pthread_join(tid[i], NULL);
	}
	sessionEndTimestamp=GetCurrentTimestamp();

	pthread_barrier_destroy(&barrier);

	printf("begin report.\n");
	EndReport(StateInfo, numTerminals);
}

/*
 * function: start function  for each thread (terminal).
 */
void runTerminal(int terminalWarehouseID, int terminalDistrictID, pthread_t *tid, pthread_barrier_t *barrier, TransState* StateInfo)
{
	int err;
	terminalArgs* args=(terminalArgs*)malloc(sizeof(terminalArgs));
	args->whse_id=terminalWarehouseID;
	args->dist_id=terminalDistrictID;
	args->type=1;

	args->barrier=barrier;
	args->StateInfo=StateInfo;

	err=pthread_create(tid,NULL,ProcStart,args);
	if(err!=0)
	{
		printf("can't create thread:%s\n",strerror(err));
		return;
	}

}

/*
 * function: load the data specially for TPCC benchmark.
 */
void dataLoading(void)
{
	int err;
	pthread_t tid;
	terminalArgs args;
	args.type=0;
	args.whse_id=100;
	args.dist_id=100;
	err=pthread_create(&tid, NULL, ProcStart, &args);
	if(err!=0)
	{
		printf("can't create thread:%s\n",strerror(err));
		return;
	}
	pthread_join(tid, NULL);
}

/*
 * function: count the tpmC and other index.
 */
void EndReport(TransState* StateInfo, int terminals)
{
	int i;
	int min, sec, msec;

	for(i=0;i<terminals;i++)
	{
		transactionCommit+=StateInfo[i].trans_commit;
		transactionAbort+=StateInfo[i].trans_abort;
		fastNewOrderCounter+=StateInfo[i].NewOrder;

	}
	transactionCount=transactionCommit+transactionAbort;

	//cut off the sleep time in the main thread.
	msec=(sessionEndTimestamp-sessionStartTimestamp)/1000-(NumTerminals*SleepTime*1000);

	tpmC=(double)((uint64_t)(6000000*fastNewOrderCounter)/msec)/100.0;
	tpmTotal=(double)((uint64_t)(6000000*transactionCount)/msec)/100.0;

	sec=(sessionEndTimestamp-sessionStartTimestamp)/1000000;
	min=sec/60;
	sec=sec%60;

	printf("tpmC = %.2lf\n",tpmC);
	printf("tpmTotal = %.2lf\n",tpmTotal);
	printf("session start at : %ld \n",sessionStartTimestamp);
	printf("session end at : %ld %ld\n",sessionEndTimestamp, sessionEndTimestamp-sessionStartTimestamp);
	printf("total rumtime is %d min, %d s\n", min, sec);
	printf("transactionCount:%d, transactionCommit:%d, transactionAbort:%d, %d %d\n",transactionCount, transactionCommit, transactionAbort, fastNewOrderCounter, transactionCount);
	for(i=0;i<terminals;i++)
	{
		printf("newOrder:%d, payment:%d, delivery:%d, orderStatus:%d, stockLevel:%d\n",StateInfo[i].NewOrder, StateInfo[i].Payment, StateInfo[i].Delivery, StateInfo[i].Order_status, StateInfo[i].Stock_level);
	}
}

