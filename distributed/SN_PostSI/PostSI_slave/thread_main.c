/*
 * threadmain.c
 *
 *  Created on: Nov 11, 2015
 *      Author: xiaoxin
 */
#include<sched.h>
#include<pthread.h>
#include<stdlib.h>
#include<fcntl.h>
#include<sys/shm.h>
#include<unistd.h>
#include<assert.h>

#include"config.h"
#include"util.h"
#include"transactions.h"
#include"proc.h"
#include"mem.h"
#include"trans.h"
#include"thread_global.h"
#include"trans_conflict.h"
#include"thread_main.h"
#include"data_record.h"
#include"lock_record.h"
#include"translist.h"
#include"lock.h"
#include"socket.h"
#include"timestamp.h"

TimeStampTz sessionStartTimestamp;

TimeStampTz sessionEndTimestamp;

uint64_t fastNewOrderCounter=0;

uint64_t transactionCount=0;

uint64_t transactionCommit=0;

uint64_t transactionAbort=0;

double tpmC, tpmTotal;

int SleepTime;

sem_t * wait_server;

void EndReport(TransState* StateInfo, int terminals);

void GetReady(void)
{
   /* get the parameters from the configure file. */
   InitNetworkParam();

   InitConfig();

   /* connect the parameter server from the master node. */
   InitParamClient();

   /* get the parameters from the master node. */
   GetParam();

   /* connect the message server from the master node,
	* the message server is used for inform the slave nodes
	* that every nodes have loaded the data. */
   InitMessageClient();

   /* the semaphore is used to synchronize the storage process and the transaction process. */
   InitSemaphore();

   InitProc();
   /* if the process-array is shared between storage and transaction, so is the process-array-lock. */
   InitProcLock();
   InitInvisibleTable();
}

void BindShmem(void)
{

   procbase=(PROC*)shmat(proc_shmid, 0, 0);

   if (procbase == (PROC*)-1)
   {
	  printf("proc shmat error.\n");
	  exit(-1);
   }

   TransConfTable = (TransConf*)shmat(invisible_shmid, 0, 0);

   if (TransConfTable == (TransConf*)-1)
   {
	  printf("invisiable shmat error.\n");
	  exit(-1);
   }
}

void InitTransaction(void)
{
	SleepTime=1;
	SetRandomSeed();
	InitMem();

	InitTransactionIdAssign();
	InitClientBuffer();
	/* initialize pthread_key_t array for thread global variables. */
	InitThreadGlobalKey();
}

void InitStorage(void)
{
	InitServerBuffer();
	InitRecord();
	InitTransactionList();
	InitServer();
}

void RunTerminals(int numTerminals)
{
	int i, j;
	int terminalWarehouseID, terminalDistrictID;

	int usedTerminal[configWhseCount][10];
	pthread_t tid[numTerminals];
	pthread_barrier_t barrier;

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

	EndReport(StateInfo, numTerminals);
}

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

/* this semaphore is used by transaction process waiting for the storage process initialize the server. */
void InitSemaphore(void)
{
	wait_server = sem_open("/wait_server", O_RDWR|O_CREAT, 00777, 0);
}

void dataLoading(void)
{
	int err;
	pthread_t tid;
	terminalArgs args;
	args.type=0;
	args.whse_id=100;
	args.dist_id=100;
	sem_wait(wait_server);
	err=pthread_create(&tid, NULL, ProcStart, &args);
	if(err!=0)
	{
		printf("can't create thread:%s\n",strerror(err));
		return;
	}
	pthread_join(tid, NULL);
}

void EndReport(TransState* StateInfo, int terminals)
{
	printf("begin report\n");
	int i;
	int min, sec, msec;
	int runabort=0, endabort=0, otherabort=0;

	for(i=0;i<terminals;i++)
	{
		transactionCommit+=StateInfo[i].trans_commit;
		transactionAbort+=StateInfo[i].trans_abort;
		fastNewOrderCounter+=StateInfo[i].NewOrder;

		runabort+=StateInfo[i].runabort;
		endabort+=StateInfo[i].endabort;
		otherabort+=StateInfo[i].otherabort;

	}
	transactionCount=transactionCommit+transactionAbort;

	msec=(sessionEndTimestamp-sessionStartTimestamp)/1000-terminals*SleepTime*1000;
    assert(msec > 0);
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
	printf("transactionCount:%d, transactionCommit:%d, transactionAbort:%d, %d %d, %d %d %d\n",transactionCount, transactionCommit, transactionAbort, fastNewOrderCounter, transactionCount, runabort, endabort, otherabort);
	for(i=0;i<terminals;i++)
	{
		printf("newOrder:%d %d, payment:%d %d, delivery:%d, orderStatus:%d, stockLevel:%d %d\n",StateInfo[i].NewOrder, StateInfo[i].NewOrder_C, StateInfo[i].Payment, StateInfo[i].Payment_C, StateInfo[i].Delivery, StateInfo[i].Order_status, StateInfo[i].Stock_level, StateInfo[i].Stock_level_C);
	}

        printf("nodenum = %d sleeptime=%d, terminals=%d\n", nodenum, SleepTime, terminals);
}
