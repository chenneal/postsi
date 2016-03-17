#include <pthread.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include "master.h"

int main()
{
	pid_t pid;
	InitNetworkParam();

	if ((pid = fork()) < 0)
		printf("fork parameter server process error\n");

	else if (pid == 0)
	{
		InitParam();
		printf("parameter server end\n");
		exit(1);
	}
	else
	{
		InitMessage();
		printf("message server end\n");
	}
	return 0;
}
