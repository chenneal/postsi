#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string.h>
#include "master.h"

int nodenum;
int threadnum;
int client_port;
int message_port;
int param_port;
char master_ip[20];

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

void InitParam(void)
{
	/* record the accept socket fd of the connected client */
	int conn;
	int param_connect;

	int param_send_buffer[3];
	int param_recv_buffer[1];

	int master_sockfd;
    int port = param_port;
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
		printf("parameter server bind error!\n");
		exit(1);
	}

    if(listen(master_sockfd, LISTEN_QUEUE) == -1)
    {
        printf("parameter server listen error!\n");
        exit(1);
    }

	socklen_t slave_length;
	struct sockaddr_in slave_addr;
	slave_length = sizeof(slave_addr);
   	int i = 0;
    while(i < nodenum)
    {
        int ret;
    	conn = accept(master_sockfd, (struct sockaddr*)&slave_addr, &slave_length);
    	param_connect = conn;

    	if(conn < 0)
    	{
    		printf("param master accept connect error!\n");
    	    exit(1);
    	}
    	i++;

    	param_send_buffer[0] = nodenum;
    	param_send_buffer[1] = threadnum;
    	param_send_buffer[2] = client_port;

    	ret = recv(param_connect, param_recv_buffer, sizeof(param_recv_buffer), 0);
    	if (ret == -1)
    	    printf("param master recv error\n");

    	ret = send(param_connect, param_send_buffer, sizeof(param_send_buffer), 0);
    	if (ret == -1)
    	    printf("param naster send error\n");

        close(param_connect);
    }
}

void InitMessage(void)
{
	/* record the accept socket fd of the connected client */
	int conn;

	int message_connect[nodenum];
	int message_buffer[1];

	int master_sockfd;
    int port = message_port;

	master_sockfd = socket(AF_INET, SOCK_STREAM, 0);
	struct sockaddr_in mastersock_addr;
	memset(&mastersock_addr, 0, sizeof(mastersock_addr));
	mastersock_addr.sin_family = AF_INET;
	mastersock_addr.sin_port = htons(port);
	mastersock_addr.sin_addr.s_addr = INADDR_ANY;

	if (bind(master_sockfd, (struct sockaddr *)&mastersock_addr, sizeof(mastersock_addr)) == -1)
	{
		printf("message server bind error!\n");
		exit(1);
	}

    if(listen(master_sockfd, LISTEN_QUEUE) == -1)
    {
        printf("message server listen error!\n");
        exit(1);
    }

	socklen_t slave_length;
	struct sockaddr_in slave_addr;
	slave_length = sizeof(slave_addr);
   	int i = 0;

    while(i < nodenum)
    {
    	conn = accept(master_sockfd, (struct sockaddr*)&slave_addr, &slave_length);
    	message_connect[i] = conn;
    	if(conn < 0)
    	{
    		printf("message master accept connect error!\n");
    	    exit(1);
    	}
    	i++;
    }

    int j;
    int ret;

    /* inform the node in the system begin start the transaction process. */
    for (j = 0; j < nodenum; j++)
    {
    	ret = recv(message_connect[j], message_buffer, sizeof(message_buffer), 0);
    	if (ret == -1)
    		printf("message master recv error\n");
    }

    for (j = 0; j < nodenum; j++)
    {
    	message_buffer[0] = 999;
    	ret = send(message_connect[j], message_buffer, sizeof(message_buffer), 0);
    	if (ret == -1)
    		printf("message master send error\n");
    	close(message_connect[j]);
    }
    /* now we can reform the node to begin to run the transaction. */
}

void InitNetworkParam(void)
{
   char buffer[5];

   ReadConfig("threadnum", buffer);
   threadnum = atoi(buffer);

   ReadConfig("nodenum", buffer);
   nodenum = atoi(buffer);

   ReadConfig("clientport", buffer);
   client_port = atoi(buffer);

   ReadConfig("paramport", buffer);
   param_port = atoi(buffer);

   ReadConfig("messageport", buffer);
   message_port = atoi(buffer);

   ReadConfig("masterip", master_ip);
}
