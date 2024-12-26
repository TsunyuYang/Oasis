#include <iostream>
#include <sstream>
#include <thread>
#include <vector>
#include <cassert>
#include <mutex>
#include <atomic>
#include <string>
#include <cstring>
#include <cstdio>
#include <cstdlib>
#include <cmath>
#include <ctime>
#include <fstream>
#include <algorithm>
#include <queue>
#include <condition_variable>

#include <bits/stdc++.h>
#include <libaio.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

using namespace std;

/*
Please note that this program assumes in-memory enviornment

Compile:
g++ mem_basic_ads_construction.cpp -lpthread -o mem_basic_ads.out

Testing:
nohup ./mem_basic_ads.out /home/yangty/graph_data/binary_graph/twitter-mpi.origin.bin ./twitter.ads 41652232 32 1>nohup.mem_basic_ads.twitter.k32.out 2>&1 &

nohup ./mem_basic_ads.out /home/yangty/graph_data/binary_graph/soc-LJ.bin ./soc-LJ.ads 4847620 32 1>nohup.mem_basic_ads.soc-LJ.k32.out 2>&1 &
*/

double wtime() {
	double time[2];	
	struct timeval time1;
	gettimeofday(&time1, NULL);

	time[0]=time1.tv_sec;
	time[1]=time1.tv_usec;

	return time[0]+time[1]*1.0e-6;
}

inline long file_size(string filename) {
	struct stat st;
	assert(stat(filename.c_str(), &st)==0);
	return st.st_size;
}

class bitmap {
public:
	unsigned num_byte;
	size_t num_item;
	char* data;
	bitmap(){
		num_byte = 0;
		num_item = 0;
		data = NULL;
	}
	~bitmap(){
		delete [] data;
	}
	
	void init(size_t size){
		num_item = size;
		num_byte = (num_item >> 3);
		if ((num_item&7) != 0)
			num_byte += 1;
		data = new char [num_byte]{};
	}
	
	void free(){
		delete [] data;
	}
	
	void clear(){ // a better way?
		for (unsigned i = 0; i < num_byte; ++i)
			data[i] = 0;
	}
	void fill() {
		for (unsigned i = 0; i < num_byte; ++i)
			data[i] = 255;
	}

	bool get_bit(size_t i) {
		assert(i < num_item);
		return data[i>>3] & (1<<(i&7));
	}
	void clear_bit(size_t i){
		assert(i < num_item);
		char val = 255 - (1<<(i&7));
		__sync_fetch_and_and(&data[i>>3], val);
	}
	void set_bit(size_t i){
		assert(i < num_item);
		__sync_fetch_and_or(&data[i>>3], (1u<<(i&7)));
	}

	void range_clear(size_t beg_idx, size_t end_idx){
		assert(beg_idx < end_idx);
		size_t cur_idx = beg_idx;
		while(cur_idx < end_idx){
			if ((cur_idx&7) == 0 && cur_idx+8 <= end_idx){
				data[cur_idx>>3] = 0;
				cur_idx += 8;
			}
			else{
				char val = 255 - (1<<(cur_idx&7));
				__sync_fetch_and_and(&data[cur_idx>>3], val);
				++cur_idx;
			}
		}
	}
};

template <typename T>
class Queue {
	const size_t capacity;
	std::queue<T> queue;
	std::mutex mutex;
	std::condition_variable cond_full;
	std::condition_variable cond_empty;
public:
	Queue(const size_t capacity) : capacity(capacity) { }
	void push(const T & item) {
		std::unique_lock<std::mutex> lock(mutex);
		cond_full.wait(lock, [&]{ return !is_full(); });
		queue.push(item);
		lock.unlock();
		cond_empty.notify_one();
	}
	T pop() {
		std::unique_lock<std::mutex> lock(mutex);
		cond_empty.wait(lock, [&]{ return !is_empty(); });
		auto item = queue.front();
		queue.pop();
		lock.unlock();
		cond_full.notify_one();
		return item;
	}
	bool is_full() {
		return queue.size()==capacity;
	}
	bool is_empty() {
		return queue.empty();
	}
};

struct ads_entry{
	unsigned vid;
	unsigned dist;
};
ads_entry make_ads_entry(unsigned input_a, unsigned input_b) {
	ads_entry u = {input_a, input_b}; 
	return u;
}

struct vid_rank{
	unsigned vid;
	unsigned rank;
};

template <class ET>
inline bool cas(ET *ptr, ET oldv, ET newv) {
	if (sizeof(ET) == 8) {
		return __sync_bool_compare_and_swap((long*)ptr, *((long*)&oldv), *((long*)&newv));
	} else if (sizeof(ET) == 4) {
		return __sync_bool_compare_and_swap((int*)ptr, *((int*)&oldv), *((int*)&newv));
	} else {
		assert(false);
	}
}
template <class ET>
inline void write_add(ET *a, ET b) {
	volatile ET newV, oldV;
	do {oldV = *a; newV = oldV + b;}
	while (!cas(a, oldV, newV));
}

//** Start Initailize Global Variables **//
int k = 64;
unsigned NUM_THD = 16;

long num_nodes = 0;
long num_edges = 0;

unsigned* graph_edges;
long* graph_idx;

unsigned* ranks;
ads_entry* all_ads;
unsigned* ads_cnt;

long klogV;
//** End Initailize Global variables **//

void read_graph(string graph_path, unsigned num_nodes)
{
	double beg_tm = wtime();
	
	long* outdeg = new long [num_nodes]{};
	long* indeg = new long [num_nodes]{};
	num_edges = file_size(graph_path) / (long)8;

	int fd = open(graph_path.c_str(), O_RDONLY);
	long buffer_sz = 8 * 1024 * 1024; // 8MB
	char* buffer = new char [buffer_sz]{};
	
	long graph_size = num_edges * 8;
	long cur_read_bytes = 0;
	while(true){
		long bytes;
		if (cur_read_bytes + buffer_sz <= graph_size){
			bytes = read(fd, buffer, buffer_sz);
			assert(bytes == buffer_sz);
		}
		else{
			long io_sz = graph_size-cur_read_bytes;
			bytes = read(fd, buffer, io_sz);
			assert(bytes == io_sz);
		}
		cur_read_bytes += bytes;
		
		long upper_idx = bytes / 8;
		for(int idx = 0; idx < upper_idx; ++idx){
			unsigned src = *(unsigned*)(buffer + idx * 8);
			unsigned dst = *(unsigned*)(buffer + idx * 8 + 4);
			outdeg[src]++;
			indeg[dst]++;
		};
		
		if (cur_read_bytes == graph_size) break;
	}
	close(fd);

	graph_edges = new unsigned [num_edges]{};
	graph_idx = new long [num_nodes+1]{};
	for(unsigned vid = 1; vid <= num_nodes; vid++){
		graph_idx[vid] = (graph_idx[vid-1] + outdeg[vid-1]);
	}

	unsigned* cur_edge_cnt = new unsigned [num_nodes]{};
	fd = open(graph_path.c_str(), O_RDONLY);
	cur_read_bytes = 0;
	while(true){
		long bytes;
		if (cur_read_bytes + buffer_sz <= graph_size){
			bytes = read(fd, buffer, buffer_sz);
			assert(bytes == buffer_sz);
		}
		else{
			long io_sz = graph_size-cur_read_bytes;
			bytes = read(fd, buffer, io_sz);
			assert(bytes == io_sz);
		}
		cur_read_bytes += bytes;
		
		long upper_idx = bytes / 8;
		for(int idx = 0; idx < upper_idx; ++idx){
			unsigned src = *(unsigned*)(buffer + idx * 8);
			unsigned dst = *(unsigned*)(buffer + idx * 8 + 4);
			
			long loc = graph_idx[src] + cur_edge_cnt[src];
			graph_edges[loc] = dst;
			++cur_edge_cnt[src];
		};
		
		if (cur_read_bytes == graph_size) break;
	}
	close(fd);
	delete [] cur_edge_cnt;
	
	// Sorting edge-list 
	vector<thread> threads;
	Queue<tuple<long, long>> tasks(65536);
	
	threads.clear();
	for(int tid = 0; tid < NUM_THD; ++tid){
		threads.emplace_back([&](int thread_id){
			while(true){
				long beg_idx, end_idx;
				tie(beg_idx, end_idx) = tasks.pop();
				if (beg_idx == -1 && end_idx == -1) break;
				sort(graph_edges + beg_idx, graph_edges + end_idx, 
					[&](unsigned left_vid, unsigned right_vid){return left_vid < right_vid; } );
			}
		}, tid);
	}
	
	for(unsigned vid = 0; vid < num_nodes; ++vid){
		if (graph_idx[vid] < graph_idx[vid+1]){
			tasks.push(make_tuple(graph_idx[vid], graph_idx[vid+1]));
		}
		else assert(graph_idx[vid] == graph_idx[vid+1]);
	}
	for(int tid = 0; tid < NUM_THD; ++tid)
		tasks.push(make_tuple(-1, -1));
	for(int tid = 0; tid < NUM_THD; ++tid)
		threads[tid].join();
	
	delete [] outdeg;
	delete [] indeg;

	cout << "Creating graph takes " << wtime() - beg_tm << " seconds\n\n";
	fflush(stdout);
}

void run_bfs(int tid)
{
	bool* visited = new bool [num_nodes]{};
	
	unsigned* active_vid =  new unsigned [num_nodes]{};
	unsigned active_cnt = 0;
	
	unsigned* nxt_active_vid = new unsigned [num_nodes]{};
	unsigned nxt_active_cnt = 0;
	
	int print_cnt = 0;
	
	//for(unsigned root = tid; root < num_nodes; root += NUM_THD){
	for(unsigned root = tid; root < 5000; root += NUM_THD){
		visited[root] = true;
		active_vid[active_cnt++] = root;
		int cur_iter = 1;
		while(active_cnt > 0){
			for(unsigned ptr = 0; ptr < active_cnt; ++ptr){
				unsigned cur_vid = active_vid[ptr];
				long beg_edge_idx = graph_idx[cur_vid];
				long end_edge_idx = graph_idx[cur_vid+1];
				while(beg_edge_idx < end_edge_idx){
					unsigned nebr = graph_edges[beg_edge_idx];
					if (visited[nebr] == false){
						visited[nebr] = true;
						nxt_active_vid[nxt_active_cnt++] = nebr;
						
						// Perform ADS Computation
						long beg_ads_idx = klogV * tid;
						if (ads_cnt[tid] < k){
							long idx = beg_ads_idx + ads_cnt[tid];
							all_ads[idx] = make_ads_entry(nebr, cur_iter);
							++ads_cnt[tid];
						}
						else{
							long end_ads_idx = beg_ads_idx + ads_cnt[tid];
							unsigned num_smaller_rank = 0;
							for(long cur_idx = beg_ads_idx; cur_idx < end_ads_idx; ++cur_idx){
								if (all_ads[cur_idx].dist <= cur_iter){
									if (ranks[all_ads[cur_idx].vid] < ranks[nebr]){
										++num_smaller_rank;
										
										if (num_smaller_rank >= k){
											cur_idx = end_ads_idx;
										}
									}
								}
							}
							
							if (num_smaller_rank < k){
								all_ads[end_ads_idx] = make_ads_entry(nebr, cur_iter);
								++ads_cnt[tid];
								assert(ads_cnt[tid] <= klogV);
							}
						}
						
					}
					++beg_edge_idx;
				}
			}
			
			active_cnt = nxt_active_cnt;
			nxt_active_cnt = 0;
			
			unsigned* tmp = active_vid;
			active_vid = nxt_active_vid;
			nxt_active_vid = tmp;
			
			++cur_iter;
		}
		
		nxt_active_cnt = 0;
		active_cnt = 0;
		for(unsigned vid = 0; vid < num_nodes; ++vid)
			visited[vid] = false;
		ads_cnt[tid] = 0;
		
		++print_cnt;
		if (tid == 0 && print_cnt >= 100000){
			cout << int(root/(double)num_nodes * 100.0) << "%" << endl;
			fflush(stdout);
			print_cnt = 0;
		}
	}
	
	delete [] visited;
	delete [] active_vid;
	delete [] nxt_active_vid;
}

// soc-LJ 4847616
// TW 41652481
int main(int argc, char ** argv)
{
	string input_path = argv[1];
	string output_path = argv[2];
	num_nodes = atol(argv[3]);
	k = atol(argv[4]);
	
	read_graph(input_path, num_nodes);

	double beg_tm = wtime();

	ranks = new unsigned [num_nodes]{};
	for(unsigned vid = 0; vid < num_nodes; ++vid)
		ranks[vid] = vid;
	srand(time(0));
	random_device rd;
	mt19937 g(rd());
	shuffle(ranks, ranks+num_nodes, g); // rank value tie-breaking
	
	klogV = k * (long)(log2(num_nodes)+5);
	// long VklogV = num_nodes * klogV;
	all_ads = new ads_entry [klogV * NUM_THD]{};
	ads_cnt = new unsigned [NUM_THD]{};
	
	thread* threads = new thread [NUM_THD]{};

	for(int tid = 0; tid < NUM_THD; ++tid)
		threads[tid] = thread(&run_bfs, tid);
	for(int tid = 0; tid < NUM_THD; ++tid)
		threads[tid].join();
	
	double end_tm = wtime();
	
	cout << "Total execution time is " << end_tm-beg_tm << " sec" << endl;
	
	delete [] threads;
	delete [] graph_edges;
	delete [] graph_idx;
	
	delete [] ranks;
	delete [] all_ads;
	delete [] ads_cnt;
	
	return 0;
}