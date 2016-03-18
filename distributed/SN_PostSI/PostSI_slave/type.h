/*
 * type.h
 *
 *  Created on: 2015-11-9
 *      Author: XiaoXin
 */
/*
 * data type is defined here.
 */
#ifndef TYPE_H_
#define TYPE_H_

#include<stdio.h>
#include<stdint.h>
#include<string.h>
#include<stdbool.h>

typedef uint32_t TransactionId;

typedef uint32_t StartId;

typedef uint32_t CommitId;

typedef uint64_t Size;

typedef uint64_t TupleId;

#define MAXINTVALUE 1<<30

/*
 * type of the command from the client, and the server process will get the different result
 * respond to different command to the client.
 */
typedef enum command
{
   cmd_insert = 1,
   cmd_trulyinsert,
   cmd_commitinsert,
   cmd_abortinsert,
   cmd_updatefind,
   cmd_updatewritelistinsert,
   cmd_updateconflict,
   cmd_updateversion,
   cmd_commitupdate,
   cmd_abortupdate,
   cmd_readfind,
   cmd_readversion,
   cmd_unrwlock,
   cmd_collisioninsert,
   cmd_getsidmin,
   cmd_updatestartid,
   cmd_updatecommitid,
   cmd_resetpair,
   cmd_release
} command;

#endif /* TYPE_H_ */
