/*
 * type.h
 *
 *  Created on: 2015��11��9��
 *      Author: DELL
 */
/*
 * data type is defined here.
 */
#ifndef TYPE_H_
#define TYPE_H_

#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <stdbool.h>

typedef uint32_t TransactionId;

typedef int StartId;

typedef int CommitId;

typedef uint64_t Size;

typedef uint64_t TupleId;

#define MAXINTVALUE 1<<30

#endif /* TYPE_H_ */
