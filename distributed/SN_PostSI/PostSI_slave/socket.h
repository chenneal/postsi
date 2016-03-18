/*
 * socket.h
 *
 *  Created on: Jan 18, 2016
 *      Author: Yu
 */

#ifndef SOCKET_H_
#define SOCKET_H_
#include "type.h"
#define NODENUM nodenum
#define THREADNUM threadnum

#define NODENUMMAX 20
#define THREADNUMMAX 64
#define LINEMAX 100

#define SEND_BUFFER_MAXSIZE 8
#define RECV_BUFFER_MAXSIZE 1000

#define SSEND_BUFFER_MAXSIZE 1000
#define SRECV_BUFFER_MAXSIZE 8

#define LISTEN_QUEUE 800

#define NODEID nodeid

typedef struct server_arg
{
   int index;
   int conn;
} server_arg;

extern int nodeid;
extern int message_port;
extern int param_port;
extern int nodenum;
extern int threadnum;
extern int port_base;

extern int message_socket;
extern int param_socket;

extern char master_ip[20];

extern void InitServer(void);
extern void InitClient(int nid, int threadid);

extern int connect_socket[NODENUMMAX][THREADNUMMAX];

extern uint64_t ** send_buffer;
extern uint64_t ** recv_buffer;
extern uint64_t ** ssend_buffer;
extern uint64_t ** srecv_buffer;

extern void InitClientBuffer(void);
extern void InitServerBuffer(void);
extern void InitParamClient(void);
extern void InitMessageClient(void);
extern void InitNetworkParam(void);
extern void GetParam(void);
extern void WaitDataReady(void);

#endif
