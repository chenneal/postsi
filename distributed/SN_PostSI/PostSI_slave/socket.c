#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string.h>
#include "thread_main.h"
#include "data.h"
#include "proc.h"
#include "socket.h"

void* Respond(void *sockp);

/* send buffer and receive buffer for client */
uint64_t ** send_buffer;
uint64_t ** recv_buffer;

/* send buffer for server respond. */
uint64_t ** ssend_buffer;
uint64_t ** srecv_buffer;

/* this array is used to save the connect with other nodes in the distributed system.
 * the connect with other slave node should be maintained until end of the process.  */

int connect_socket[NODENUMMAX][THREADNUMMAX];

int threadnum;
int nodenum;

int message_socket;
int param_socket;

int nodeid;

/* the number of nodes should not be larger than NODE NUMBER MAX */
char node_ip[NODENUMMAX][20];

int message_port;
int param_port;
int port_base;

char master_ip[20];

pthread_t * server_tid;

/* read the configure parameters from the configure file. */
int ReadConfig(char * find_string, char * result)
{
   int i;
   int j;
   int k;
   FILE * fp;
   char buffer[30];
   char * p;
   if ((fp = fopen("config.txt", "r")) == NULL)
   {
       printf("can not open the configure file.\n");
       fclose(fp);
       return -1;
   }
   for (i = 0; i < LINEMAX; i++)
   {
      if (fgets(buffer, sizeof(buffer), fp) == NULL)
         continue;
      for (p = find_string, j = 0; *p != '\0'; p++, j++)
      {
         if (*p != buffer[j])
            break;
      }

      if (*p != '\0' || buffer[j] != ':')
      {
         continue;
      }

      else
      {
         k = 0;
         /* jump over the character ':' and the space character. */
         j = j + 2;
         while (buffer[j] != '\0')
         {
            *(result+k) = buffer[j];
            k++;
            j++;
         }
         *(result+k) = '\0';
         fclose(fp);
         return 1;
      }
   }
   fclose(fp);
   printf("can not find the configure you need\n");
   return -1;
}

void InitClientBuffer(void)
{
	int i;

	send_buffer = (uint64_t **) malloc (THREADNUM * sizeof(uint64_t *));
	recv_buffer = (uint64_t **) malloc (THREADNUM * sizeof(uint64_t *));

	if (send_buffer == NULL || recv_buffer == NULL)
		printf("client buffer pointer malloc error\n");

	for (i = 0; i < THREADNUM; i++)
	{
		send_buffer[i] = (uint64_t *) malloc (SEND_BUFFER_MAXSIZE * sizeof(uint64_t));
		recv_buffer[i] = (uint64_t *) malloc (RECV_BUFFER_MAXSIZE * sizeof(uint64_t));
		if ((send_buffer[i] == NULL) || recv_buffer[i] == NULL)
			printf("client buffer malloc error\n");
	}
}

void InitServerBuffer(void)
{
	int i;
	ssend_buffer = (uint64_t **) malloc ((NODENUM*THREADNUM+1) * sizeof(uint64_t *));
	srecv_buffer = (uint64_t **) malloc ((NODENUM*THREADNUM+1) * sizeof(uint64_t *));

	if (ssend_buffer == NULL || srecv_buffer == NULL)
		printf("server buffer pointer malloc error\n");

	for (i = 0; i < NODENUM*THREADNUM+1; i++)
	{
	   ssend_buffer[i] = (uint64_t *) malloc (SSEND_BUFFER_MAXSIZE * sizeof(uint64_t));
	   srecv_buffer[i] = (uint64_t *) malloc (SRECV_BUFFER_MAXSIZE * sizeof(uint64_t));
	   if ((ssend_buffer[i] == NULL) || srecv_buffer[i] == NULL)
		  printf("server buffer malloc error\n");
	}
}

void InitParamClient(void)
{
	int slave_sockfd;
	slave_sockfd = socket(AF_INET, SOCK_STREAM, 0);
    int port = param_port;
    struct sockaddr_in mastersock_addr;
	memset(&mastersock_addr, 0, sizeof(mastersock_addr));
	mastersock_addr.sin_addr.s_addr = inet_addr(master_ip);
	mastersock_addr.sin_family = AF_INET;
	mastersock_addr.sin_port = htons(port);

	if (connect(slave_sockfd, (struct sockaddr *)&mastersock_addr, sizeof(mastersock_addr)) < 0)
	{
		printf("client master param connect error!\n");
	    exit(1);
	}

	param_socket = slave_sockfd;
}

void InitMessageClient(void)
{
	int slave_sockfd;
	slave_sockfd = socket(AF_INET, SOCK_STREAM, 0);
    int port = message_port;
    struct sockaddr_in mastersock_addr;
	memset(&mastersock_addr, 0, sizeof(mastersock_addr));
	mastersock_addr.sin_addr.s_addr = inet_addr(master_ip);
	mastersock_addr.sin_family = AF_INET;
	mastersock_addr.sin_port = htons(port);

	if (connect(slave_sockfd, (struct sockaddr *)&mastersock_addr, sizeof(mastersock_addr)) < 0)
	{
		printf("client master message connect error!\n");
	    exit(1);
	}

	message_socket = slave_sockfd;
}

/* nid is the id of node in the distributed system */
void InitClient(int nid, int threadid)
{
	int slave_sockfd;
	// use the TCP protocol
	slave_sockfd = socket(AF_INET, SOCK_STREAM, 0);
    int port = port_base + nid;
    struct sockaddr_in mastersock_addr;
	memset(&mastersock_addr, 0, sizeof(mastersock_addr));
	mastersock_addr.sin_addr.s_addr = inet_addr(node_ip[nid]);
	mastersock_addr.sin_family = AF_INET;
	mastersock_addr.sin_port = htons(port);

	if (connect(slave_sockfd, (struct sockaddr *)&mastersock_addr, sizeof(mastersock_addr)) < 0)
	{
		printf("client connect error!\n");
	    exit(1);
	}

	connect_socket[nid][threadid] = slave_sockfd;
}

void InitServer(void)
{
	int status;
	/* record the accept socket fd of the connected client */
	int conn;
	server_tid = (pthread_t *) malloc ((NODENUM*THREADNUM+1)*sizeof(pthread_t));
	server_arg *argu = (server_arg *)malloc((NODENUM*THREADNUM+1)*sizeof(server_arg));
	int master_sockfd;
    int port = port_base + nodeid;
	/* use the TCP protocol */
	master_sockfd = socket(AF_INET, SOCK_STREAM, 0);
	//bind
	struct sockaddr_in mastersock_addr;
	memset(&mastersock_addr, 0, sizeof(mastersock_addr));
	mastersock_addr.sin_family = AF_INET;
	mastersock_addr.sin_port = htons(port);
	mastersock_addr.sin_addr.s_addr = INADDR_ANY;

	if (bind(master_sockfd, (struct sockaddr *)&mastersock_addr, sizeof(mastersock_addr)) == -1)
	{
		printf("bind error!\n");
		exit(1);
	}
	/* listen */
    if(listen(master_sockfd, LISTEN_QUEUE) == -1)
    {
        printf("listen error!\n");
        exit(1);
    }

    /* Important here , the transaction process should always wait for the server initialization */
    sem_post(wait_server);
    /* receive or transfer data */
	socklen_t slave_length;
	struct sockaddr_in slave_addr;
	slave_length = sizeof(slave_addr);

   	int i = 0;
    while(i < (NODENUM*THREADNUM+1))
    {
    	conn = accept(master_sockfd, (struct sockaddr*)&slave_addr, &slave_length);
    	argu[i].conn = conn;
    	argu[i].index = i;
    	if(conn < 0)
    	{
    		printf("server connect error!\n");
    	    exit(1);
    	}
    	status = pthread_create(&server_tid[i], NULL, Respond, &(argu[i]));
    	if (status != 0)
    		printf("create thread %d error %d!\n", i, status);
    	i++;
    }

    /* wait the child threads to complete. */
    for (i = 0; i < NODENUM*THREADNUM+1; i++)
       pthread_join(server_tid[i], NULL);
}

void* Respond(void *pargu)
{
	int conn;
	int index;
	server_arg * temp;
	temp = (server_arg *) pargu;
	conn = temp->conn;
	index = temp->index;

	memset(srecv_buffer[index], 0, SRECV_BUFFER_MAXSIZE*sizeof(uint64_t));
	int ret;
	command type;
    do
    {
   	    ret = recv(conn, srecv_buffer[index], SRECV_BUFFER_MAXSIZE*sizeof(uint64_t), 0);

   	    if (ret == -1)
   	    {
   	    	printf("server receive error!\n");
   	    }
   	    type = (command)(*(srecv_buffer[index]));
   	    switch(type)
   	    {
   	       case cmd_insert:
   	    	   ProcessInsert(srecv_buffer[index], conn, index);
   	    	   break;
   	       case cmd_trulyinsert:
   	    	   ProcessTrulyInsert(srecv_buffer[index], conn, index);
   	    	   break;
   	       case cmd_commitinsert:
   	    	   ProcessCommitInsert(srecv_buffer[index], conn, index);
   	    	   break;
   	       case cmd_abortinsert:
   	    	   ProcessAbortInsert(srecv_buffer[index], conn, index);
   	    	   break;
   	       case cmd_updatefind:
               ProcessUpdateFind(srecv_buffer[index], conn, index);
               break;
   	       case cmd_updateconflict:
   	    	   ProcessUpdateConflict(srecv_buffer[index], conn, index);
   	    	   break;
   	       case cmd_updateversion:
   	    	   ProcessUpdateVersion(srecv_buffer[index], conn, index);
   	    	   break;
   	       case cmd_commitupdate:
   	    	   ProcessCommitUpdate(srecv_buffer[index], conn, index);
   	    	   break;
   	       case cmd_abortupdate:
   	    	   ProcessAbortUpdate(srecv_buffer[index], conn, index);
   	    	   break;
   	       case cmd_readfind:
   	    	   ProcessReadFind(srecv_buffer[index], conn, index);
   	    	   break;
   	       case cmd_unrwlock:
   	    	   ProcessUnrwLock(srecv_buffer[index], conn, index);
   	    	   break;
   	       case cmd_collisioninsert:
   	    	   ProcessCollisionInsert(srecv_buffer[index], conn, index);
   	    	   break;
   	       case cmd_readversion:
   	    	   ProcessReadVersion(srecv_buffer[index], conn, index);
   	    	   break;
   	       case cmd_getsidmin:
   	    	   ProcessGetSidMin(srecv_buffer[index], conn, index);
   	    	   break;
   	       case cmd_updatestartid:
   	    	   ProcessUpdateStartId(srecv_buffer[index], conn, index);
   	    	   break;
   	       case cmd_updatecommitid:
   	    	   ProcessUpdateCommitId(srecv_buffer[index], conn, index);
   	    	   break;
   	       case cmd_resetpair:
   	    	   ProcessRestPair(srecv_buffer[index], conn, index);
   	    	   break;
   	       case cmd_release:
   	    	   printf("release the connect\n");
   	    	   break;
   	       default:
   	    	   printf("error route, never here!\n");
   	    	   break;
   	    }
    } while (type != cmd_release);
	close(conn);
    pthread_exit(NULL);
    return (void*)NULL;
}

void InitNetworkParam(void)
{
   int i;
   char buffer[5];
   char node[10];

   ReadConfig("nodeid", buffer);
   nodeid = atoi(buffer);

   ReadConfig("messageport", buffer);
   message_port = atoi(buffer);

   ReadConfig("paramport", buffer);
   param_port = atoi(buffer);

   ReadConfig("masterip", master_ip);

   for (i = 0; i < NODENUMMAX; i++)
   {
      sprintf(node, "nodeip%d", i);
      if (ReadConfig(node, node_ip[i]) == -1)
    	  break;
   }
}

void GetParam(void)
{
   int param_send_buffer[1];
   int param_recv_buffer[3];
   param_send_buffer[0] = 999;

   if (send(param_socket, param_send_buffer, sizeof(param_send_buffer), 0) == -1)
	   printf("get param send error\n");
   if (recv(param_socket, param_recv_buffer, sizeof(param_recv_buffer), 0) == -1)
	   printf("get param recv error\n");

   nodenum = param_recv_buffer[0];
   threadnum = param_recv_buffer[1];
   port_base = param_recv_buffer[2];
}

void WaitDataReady(void)
{
	int wait_buffer[1];
	wait_buffer[0] = 999;

	if (send(message_socket, wait_buffer, sizeof(wait_buffer), 0) == -1)
		printf("wait data send error\n");
	if (recv(message_socket, wait_buffer, sizeof(wait_buffer), 0) == -1)
		printf("wait data recv error\n");
}
