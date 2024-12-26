#include <iostream>
#include <thread>
#include <vector>
#include <cassert>
#include <mutex>
#include <atomic>
#include <string>
#include <cstring>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <algorithm>
#include <condition_variable>
#include <queue>

#include <bits/stdc++.h>
#include <libaio.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <malloc.h>

using namespace std;

int main()
{
	string path1 = "/home/yangty/ADS/data/soc-LJ.origin.32p.grid_2Mblock/";
	string path2 = "/home/yangty/ADS/data/soc-LJ.origin.128p.grid_2Mblock/";
	
	unsigned num_nodes = 4847616;
	
	unsigned* ranks = new unsigned [num_nodes]{};
	
	for(unsigned vid = 0; vid < num_nodes; ++vid)
		ranks[vid] = vid;
	srand(0); // -> srand(time(0));
	random_device rd;
	mt19937 g(rd());
	shuffle(ranks, ranks+num_nodes, g); // rank value tie-breaking
	
	int fout_ranks = open((path1+"ranks").c_str(), O_WRONLY|O_APPEND|O_CREAT|O_TRUNC);
	for(unsigned vid = 0; vid < num_nodes; ++vid){
		/*long bytes = 0;
		while(bytes != sizeof(unsigned)){
			bytes = write(fout_ranks, &ranks[vid], sizeof(unsigned));
		}*/
		long bytes = write(fout_ranks, &ranks[vid], sizeof(unsigned));
		assert(bytes == sizeof(unsigned));
	}
	close(fout_ranks);
	
	fout_ranks = open((path2+"ranks").c_str(), O_WRONLY|O_APPEND|O_CREAT|O_TRUNC);
	for(unsigned vid = 0; vid < num_nodes; ++vid){
		/*long bytes = 0;
		while(bytes != sizeof(unsigned)){
			bytes = write(fout_ranks, &ranks[vid], sizeof(unsigned));
		}*/
		long bytes = write(fout_ranks, &ranks[vid], sizeof(unsigned));
		assert(bytes == sizeof(unsigned));
	}
	close(fout_ranks);
	
	
	return 0;
}