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
g++ mem_advanced_ads_construction.cpp -lpthread -o mem_advanced_ads.out

Testing:
nohup ./mem_advanced_ads.out /home/yangty/graph_data/binary_graph/twitter-mpi.origin.bin ./twitter.mem_ads.k64/ 41652232 64 1>nohup.mem_advanced_ads.twitter.k64.out 2>&1 &

nohup ./mem_advanced_ads.out /home/yangty/graph_data/binary_graph/soc-LJ.bin ./soc-LJ.k32.ads/ 4847620 32 1>nohup.mem_advanced_ads.soc-LJ.k32.out 2>&1 &

nohup ./mem_advanced_ads.out /home/yangty/graph_data/binary_graph/soc-LJ.bin ./soc-LJ.k4.ads/ 4847620 4 1>nohup.mem_advanced_ads.soc-LJ.k4.out 2>&1 &

nohup ./mem_advanced_ads.out /home/yangty/graph_data/binary_graph/soc-LJ.bin ./soc-LJ.k8.ads/ 4847620 8 1>nohup.mem_advanced_ads.soc-LJ.k8.out 2>&1 &
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

unsigned* rev_edges;
long* rev_idx;

unsigned* ranks;
ads_entry* all_ads;
unsigned* ads_cnt;

unsigned** ads_vid_result;
unsigned max_vid_per_thd;
bool* inserted;
//** End Initailize Global variables **//

class ads_struct{ // distance isn't recorded for simplicity
public:
	ads_entry* top_k_ads; // top k smallest ads
	vector<ads_entry> rest_ads;
	unsigned num_ads;
	
	ads_struct(){
		num_ads = 0;
		top_k_ads =  new ads_entry [k]{};
	}
	
	~ads_struct(){
		delete [] top_k_ads;
	}
	
	bool insert(unsigned vid, unsigned cur_dist){
		if (num_ads < k){
			ads_entry tmp; tmp.vid = vid; tmp.dist = cur_dist;
			top_k_ads[num_ads++] = tmp;
			return true;
		}
		else {
			unsigned largest_rank = 0;
			int idx = -1;
			for(int i = 0; i < k; ++i){
				if (largest_rank < ranks[top_k_ads[i].vid]){
					largest_rank = ranks[top_k_ads[i].vid];
					idx = i;
				}
			}
			if (ranks[vid] < largest_rank){
				ads_entry tmp; tmp.vid = vid; tmp.dist = cur_dist;
				
				assert(idx != -1);
				rest_ads.push_back(top_k_ads[idx]);
				top_k_ads[idx] = tmp;
				num_ads++;
				return true;
			}
			else return false;
		}
	}
};

void read_reverse_graph(string graph_path, unsigned num_nodes)
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

	rev_edges = new unsigned [num_edges]{};
	rev_idx = new long [num_nodes+1]{};
	for(unsigned vid = 1; vid <= num_nodes; vid++){
		rev_idx[vid] = (rev_idx[vid-1] + indeg[vid-1]);
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
			
			long loc = rev_idx[dst] + cur_edge_cnt[dst];
			rev_edges[loc] = src;
			++cur_edge_cnt[dst];
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
				sort(rev_edges + beg_idx, rev_edges + end_idx, 
					[&](unsigned left_vid, unsigned right_vid){return left_vid < right_vid; } );
			}
		}, tid);
	}
	
	for(unsigned vid = 0; vid < num_nodes; ++vid){
		if (rev_idx[vid] < rev_idx[vid+1]){
			tasks.push(make_tuple(rev_idx[vid], rev_idx[vid+1]));
		}
		else assert(rev_idx[vid] == rev_idx[vid+1]);
	}
	for(int tid = 0; tid < NUM_THD; ++tid)
		tasks.push(make_tuple(-1, -1));
	for(int tid = 0; tid < NUM_THD; ++tid)
		threads[tid].join();
	
	delete [] outdeg;
	delete [] indeg;

	cout << "Creating reversed graph takes " << wtime() - beg_tm << " seconds\n\n";
	fflush(stdout);
}

// soc-LJ 4847616
// TW 41652481
int main(int argc, char ** argv)
{
	string input_path = argv[1];
	string output_path = argv[2];
	num_nodes = atol(argv[3]);
	k = atol(argv[4]);
	
	read_reverse_graph(input_path, num_nodes);

	double beg_tm = wtime();

	ranks = new unsigned [num_nodes]{};
	for(unsigned vid = 0; vid < num_nodes; ++vid)
		ranks[vid] = vid;
	srand(time(0));
	random_device rd;
	mt19937 g(rd());
	shuffle(ranks, ranks+num_nodes, g); // rank value tie-breaking
	
	vid_rank* exe_order = new vid_rank [num_nodes]{};
	for(unsigned vid = 0; vid < num_nodes; ++vid){
		exe_order[vid].vid = vid;
		exe_order[vid].rank = ranks[vid];
	}
	sort(exe_order, exe_order + num_nodes,
		[&](vid_rank a, vid_rank b){return a.rank < b.rank;} );
	
	long klogV = k * (long)(log2(num_nodes)+5);
	long VklogV = num_nodes * klogV;
	all_ads = new ads_entry [VklogV]{};
	ads_cnt = new unsigned [num_nodes]{};
	
	vector<thread> threads;
	Queue<tuple<unsigned, unsigned, unsigned>> tasks(65536);
	int sw1 = 0, sw2 = 0;
	
	max_vid_per_thd = num_nodes / NUM_THD + 1;
	ads_vid_result = new unsigned* [NUM_THD]{};
	for(int tid = 0; tid < NUM_THD; ++tid){
		ads_vid_result[tid] = new unsigned [max_vid_per_thd]{};
	}
	inserted = new bool [num_nodes]{};
	
	bool* visited = new bool [num_nodes]{};
	unsigned* active_nodes = new unsigned [num_nodes]{};
	unsigned active_cnt = 0;
	unsigned* nxt_active_nodes = new unsigned [num_nodes]{};
	unsigned nxt_active_cnt = 0;
	unsigned root;
	
	unsigned total_active_cnt = 0;
	unsigned* active_nodes_per_thd = new unsigned [num_nodes * NUM_THD]{};
	unsigned* active_cnt_per_thd = new unsigned [NUM_THD]{};
	
	threads.clear();
	for(int tid = 0; tid < NUM_THD; ++tid){
		threads.emplace_back([&](int thread_id){
			while(true){
				unsigned active_beg_idx, active_end_idx, dist;
				tie(active_beg_idx, active_end_idx, dist) = tasks.pop();
				
				if (active_beg_idx == -1 && active_end_idx == 0 && dist == 0){
					write_add(&sw1, 1);
					while(sw1 != 0) { __asm volatile ("pause" ::: "memory"); };
					write_add(&sw2, 1);
					
					continue;
				}
				else if (active_beg_idx == -1 && active_end_idx == -1 && dist == -1){
					break;
				}

				while(active_beg_idx < active_end_idx){
					unsigned targ_vid = nxt_active_nodes[active_beg_idx++];
					unsigned src_vid = root;
					
					// Perform ADS Computation
					long beg_ads_idx = klogV * targ_vid;
					bool success = false;
					if (ads_cnt[targ_vid] < k){
						long idx = beg_ads_idx + ads_cnt[targ_vid];
						all_ads[idx] = make_ads_entry(src_vid, dist);
						++ads_cnt[targ_vid];
						success = true;
					}
					else{
						long end_ads_idx = beg_ads_idx + ads_cnt[targ_vid];
						unsigned num_smaller_rank = 0;
						for(long cur_idx = beg_ads_idx; cur_idx < end_ads_idx; ++cur_idx){
							if (all_ads[cur_idx].dist <= dist){
								if (ranks[all_ads[cur_idx].vid] < ranks[src_vid]){
									++num_smaller_rank;
									
									if (num_smaller_rank >= k){
										cur_idx = end_ads_idx;
									}
								}
							}
						}
						
						if (num_smaller_rank < k){
							all_ads[end_ads_idx] = make_ads_entry(src_vid, dist);
							++ads_cnt[targ_vid];
							success = true;
							assert(ads_cnt[targ_vid] <= klogV);
						}
					}
					
					if (success){
						//inserted[targ_vid] = true;
						active_nodes_per_thd[num_nodes * thread_id + active_cnt_per_thd[thread_id]] = targ_vid;
						++active_cnt_per_thd[thread_id];
					}
					//else inserted[targ_vid] = false;
				}
			}
		}, tid);
	}

	for(unsigned exe_idx = 0; exe_idx < num_nodes; ++exe_idx){
		double iter_beg_time = wtime();
		root = exe_order[exe_idx].vid; // Use this root to perform BFS/Dijkstra
		
		unsigned cur_iter = 1;
		visited[root] = true;
		//active_nodes[active_cnt++] = root;
		
		active_nodes_per_thd[0] = root;
		++active_cnt_per_thd[0];
		++total_active_cnt;
		
		while (total_active_cnt > 0){ // when there are still active vertices

			for(int master_tid = 0; master_tid < NUM_THD; ++master_tid){
				if (active_cnt_per_thd[master_tid] > 0){
					long beg_idx = master_tid * num_nodes;
					long end_idx = beg_idx + active_cnt_per_thd[master_tid];
					while(beg_idx < end_idx){
						unsigned active_vid = active_nodes_per_thd[beg_idx];
						long beg_edge_idx = rev_idx[active_vid];
						long end_edge_idx = rev_idx[active_vid+1];
						for(long cur_edge_idx = beg_edge_idx; cur_edge_idx < end_edge_idx; ++cur_edge_idx){
							unsigned nebr = rev_edges[cur_edge_idx];
							if (visited[nebr] == false){
								nxt_active_nodes[nxt_active_cnt++] = nebr;
								visited[nebr] = true;
							}
						}
						++beg_idx;
					}
					
					active_cnt_per_thd[master_tid] = 0;
				}
			}
		
			/*unsigned active_idx = 0;
			while(active_idx < active_cnt){
				unsigned active_vid = active_nodes[active_idx];
				long beg_edge_idx = rev_idx[active_vid];
				long end_edge_idx = rev_idx[active_vid+1];
				for(long cur_edge_idx = beg_edge_idx; cur_edge_idx < end_edge_idx; ++cur_edge_idx){
					unsigned nebr = rev_edges[cur_edge_idx];
					if (visited[nebr] == false){
						nxt_active_nodes[nxt_active_cnt++] = nebr;
						visited[nebr] = true;
					}
				}
				++active_idx;
			}*/
			
			unsigned num_v_per_thread = nxt_active_cnt / NUM_THD;
			for(unsigned tid = 0; tid < NUM_THD; ++tid){
				unsigned active_beg_idx = tid * num_v_per_thread;
				unsigned active_end_idx = (tid+1) * num_v_per_thread;
				if (active_end_idx > nxt_active_cnt)
					active_end_idx = nxt_active_cnt;
				tasks.push(make_tuple(active_beg_idx, active_end_idx, cur_iter));
			}

			sw1 = 0;
 			for(unsigned tid = 0; tid < NUM_THD; ++tid){
				tasks.push(make_tuple(-1, 0, 0));
			}
			while(sw1 != NUM_THD) { __asm volatile ("pause" ::: "memory"); }

			sw1 = 0;
			while(sw2 != NUM_THD) { __asm volatile ("pause" ::: "memory"); }
			sw2 = 0;

			/*nxt_active_cnt = 0;
			active_cnt = 0;
			for(unsigned vid = 0; vid < num_nodes; ++vid){
				if (inserted[vid]){
					active_nodes[active_cnt++] = vid;
					inserted[vid] = false;
				}
			}*/
			
			nxt_active_cnt = 0;
			total_active_cnt = 0;
			for(int tid = 0; tid < NUM_THD; ++tid)
				total_active_cnt += active_cnt_per_thd[tid];
			
			++cur_iter; 
		}

		// Prepare for next BFS/Dijkstra
		for(unsigned vid = 0; vid < num_nodes; ++vid)
			visited[vid] = false;
		active_cnt = 0;
		nxt_active_cnt = 0;
		total_active_cnt = 0;
		for(int tid = 0; tid < NUM_THD; ++tid)
			total_active_cnt += active_cnt_per_thd[tid];

		double iter_end_time = wtime();
		if ((exe_idx%10000) == 0){
			cout << exe_idx << ", " << iter_end_time - iter_beg_time << " sec\n";
			fflush(stdout);
		}
	}
	double end_tm = wtime();
	cout << "Constructing ADS totally takes " << end_tm - beg_tm << " seconds" << endl;
	
	for(int tid = 0; tid < NUM_THD; ++tid)
		tasks.push(make_tuple(-1, -1, -1));
	for(int tid = 0; tid < NUM_THD; ++tid)
		threads[tid].join();

	fstream fw(output_path+"ads_meta", ios::out | ios::trunc);
	fw << num_nodes << " ";
	fw << num_edges << " ";
	fw << k << " ";
	fw << klogV << " ";
	fw.close();
	
	FILE* fw_all_ads;
	fw_all_ads = fopen((output_path+"all_ads").c_str(), "wb");
	fwrite(all_ads, sizeof(ads_entry), VklogV, fw_all_ads);
	fclose(fw_all_ads);
	
	FILE* fw_ads_cnt;
	fw_ads_cnt = fopen((output_path+"ads_cnt").c_str(), "wb");
	fwrite(ads_cnt, sizeof(unsigned), num_nodes, fw_ads_cnt);
	fclose(fw_ads_cnt);
	
	FILE* fw_ranks;
	fw_ranks = fopen((output_path+"ranks").c_str(), "wb");
	fwrite(ranks, sizeof(unsigned), num_nodes, fw_ranks);
	fclose(fw_ranks);

	delete [] active_nodes;
	delete [] nxt_active_nodes;
	delete [] exe_order;
	delete [] visited;
	
	delete [] ranks;
	delete [] all_ads;
	delete [] ads_cnt;
	return 0;

	
	
	
	
	
	
	
	
	/*for(unsigned root = 0; root < exe_range; ++root){ // Naive ADS Construction
		if (out_off[root+1] == out_off[root]) continue;
		// if (exe_nodes[root] == false) continue;
		
		int cur_iter = 1;
		bool* states = new bool [num_nodes]{};
		states[root] = true;
		unsigned* cur_active = new unsigned [num_nodes]{};
		unsigned cur_active_idx = 0;
		unsigned* nxt_active = new unsigned [num_nodes]{};
		unsigned nxt_active_idx = 0;
		
		cur_active[cur_active_idx++] = root;
		while(cur_active_idx != 0){
			
			//cout << "a-1" << endl;
			// 1. Standard BFS
			for(int i = 0; i < cur_active_idx; i++){ 
				unsigned active_node = cur_active[i];
				assert(states[active_node] == true);
				
				long beg_idx = out_off[active_node];
				long end_idx = out_off[active_node+1];
				while(beg_idx < end_idx){
					unsigned out_nebr = outedges[beg_idx];
					assert(out_nebr < num_nodes);
					if (states[out_nebr] == false){
						states[out_nebr] = true;
						nxt_active[nxt_active_idx++] = out_nebr;
					}
					++beg_idx;
				}
			}
			cur_active_idx = nxt_active_idx;
			nxt_active_idx = 0;
			swap(cur_active, nxt_active);
			
			
			//cout << "a-2" << endl;
			// 2. Creating ADS
			if (cur_active_idx != 0){
				//sort(cur_active.begin(), cur_active.end()); // distance tie-breaking; given d(a,b)==d(a,c), path(a,b) < path(a,c) if b < c
 				for(int i = 0; i < cur_active_idx; i++){
					unsigned trav_node = cur_active[i];
					all_ads[root].insert(trav_node, cur_iter); // find out k-th smallest rank & insert in ADS(root)
				}
			}
			
			++cur_iter;
			//cout << cur_active_idx << endl;
		}
		
		cout << graph_name << ", " << k << ", " << root << ", " << all_ads[root].num_ads << endl;
		
		delete [] states;
		delete [] cur_active;
		delete [] nxt_active;
	}
	double end_tm = wtime();
	

	long bytes;
	// write ranks to file
	string k_str = to_string(k);
	string output_path = path+graph_name+"."+k_str;
	int fd_rank = open((output_path+".ranks").c_str(), O_WRONLY | O_CREAT | O_TRUNC);
	bytes = write(fd_rank, ranks, num_nodes * sizeof(unsigned));
	assert(bytes == num_nodes * sizeof(unsigned));
	close(fd_rank);

	// write meta to file
	FILE * fmeta = fopen((output_path+".meta").c_str(), "w");
	fprintf(fmeta, "%d %ld %d", num_nodes, num_edges, exe_range);
	fclose(fmeta);
	
	// write ADS list to file
	long* ads_idx = new long [exe_range+1]{};
	for(unsigned vid = 0; vid < exe_range ; ++vid){
		ads_idx[vid+1] = (ads_idx[vid] + all_ads[vid].num_ads);
	}
	int fd_ads_idx = open((output_path+".ads_idx").c_str(), O_WRONLY | O_CREAT | O_TRUNC);
	bytes = write(fd_ads_idx, ads_idx, (exe_range+1) * sizeof(long));
	assert(bytes == (exe_range+1) * sizeof(long));
	close(fd_ads_idx);
	
	int fd_ads = open((output_path+".ads").c_str(), O_WRONLY | O_CREAT | O_TRUNC);
	for(unsigned vid = 0; vid < exe_range ; ++vid){
		long write_bytes = k * sizeof(ads_entry);
		long bytes = write(fd_ads, all_ads[vid].top_k_ads, write_bytes);
		assert(bytes == write_bytes);
		
		write_bytes = all_ads[vid].rest_ads.size() * sizeof(ads_entry);
		//bytes = write(fd_ads, all_ads[vid].rest_ads.begin(), write_bytes);
		bytes = write(fd_ads, (void*)all_ads[vid].rest_ads.data(), write_bytes);
		assert(bytes == write_bytes);
	}
	close(fd_ads);*/

	return 0;
}