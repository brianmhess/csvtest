#include <stdio.h>
#include <stdlib.h>

int main(int arg, char **argv) {
	int numrecs = 1000000;
	int numcols = 8;
	int range[8] = {100, 100, 100, 100, 200, 200, 200, 200};
	int keyrange = 10000;
	int blobbed = atoi(argv[1]);
	char delim = (0 == blobbed) ? ',' : '|';
	int j;
	long long i;

	srand48(0);
	for (i = 0; i < numrecs; i++) {
		printf("%d,%lld,%d", (int)(drand48() * keyrange), i, (int)(drand48() * range[0]));
		for (j = 1; j < numcols; j++)
			printf("%c%d", delim, (int)(drand48() * range[j]));
		printf("\n");
	}

	return 0;
}
