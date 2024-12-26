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

long max_ele_per_buff = (long)32*1024*1024;
//typedef unsigned int vid_type
//typedef unsigned int ew_type

/*****
Please note that baseline_ads_construction is based on DP-based scheme
g++ ooc_baseline_ads_construction.cpp -static-libstdc++ -lpthread -laio
For testing: ./exe binary_graph_path k NUM_THD
E.g., sudo nohup ./a.out /data/nvme_raid/yangty/twitter.256p.grid/ 16 16 1>nohup.tw.ooc_baseline.out 2>&1 &
// sudo ./a.out /data/nvme_raid/yangty/twitter.256p.grid/ 16 16 
*****/

mutex* buff_mtx;
thread* threads;

int* fd_rd_upd;
long* fd_rd_upd_size;

int* fd_wt_upd;
long* fd_wt_upd_size;

int* fd_ads;
long* fd_ads_size;
//int fd_ads;
//long fd_ads_size;

long* column_offset;
int fd_edge;

int* fd_top_k_ads;

mutex mtx_fwt;

char* ads_buffer_pool;
char* upd_buffer_pool;
long buffer_size = 3 * 1024 * 1024;
long batch_size = 3 * 1024 * 1024;

char** edge_buffer;
char* edge_buffer_pool;

char* buffer_top_k_ads;
long top_k_total_sz;

char* buffer_new_ads;
long new_ads_total_sz;

double wtime()
{
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

struct ads_entry{
	unsigned src, dst;
	unsigned dist;
};

struct ads_update{
	unsigned src, dst;
	unsigned dist;
};


class my_buffer{ // each thread own a passing buffer
public:
	ads_entry* mem_buffer;
	long num_entry;
	long max_entry_per_buffer;
	//long max_in_mem_upd;
	
	//int buffer_id;
	
	// backup file
	// string dir;
	// string* fname; 
	// Each thread owns an indepedent passing_buffer (mutex-free), .. 
	// .. and change the fname to swtich to different grids
	// e.g., suppose ID range = [x .. y] in a grid & # threads = 16
	// passing_buffer of tid=0 is responsible for [x .. x+(y-x)/16] for all x and y in different grids.

public:
	my_buffer(){
		num_entry = 0;
		max_entry_per_buffer = buffer_size / sizeof(ads_entry);
		assert((buffer_size%sizeof(ads_entry))==0);
	}
	
	~my_buffer(){
		// do nothing
	}
	
	void init(string path, int id){
		
	}
	
	void flush(){ // flush the rest of updates in mem_buffer to fd(part_id)
		num_entry = 0;
	}
	
	void chunk_read(int fd, long beg_off, long end_off){ // for fd_rd_upd
		assert((beg_off%512)==0);
		long length = end_off - beg_off;
		long io_length = ((length%512)==0)? length: (length/512+1)*512;
		
		long bytes = pread(fd, mem_buffer, io_length, beg_off);
		assert(bytes == length);
	}
	
	void mutex_free_entry_write(int fd, long* fd_size, ads_entry tmp){ // for fd_wt_upd
		if (num_entry == max_entry_per_buffer){
			assert(((*fd_size)%512)==0);
			
			long bytes = pwrite(fd, mem_buffer, buffer_size, (*fd_size));
			assert(bytes==buffer_size);
			
			(*fd_size) += buffer_size;
			num_entry = 0;
		}
		mem_buffer[num_entry++] = tmp;
	}
	
	void entry_write(int fd, long* fd_size, ads_entry tmp){ // for fd_ads
		//return;
	
		if (num_entry == max_entry_per_buffer){
			long beg_off;
			
			mtx_fwt.lock();
			//beg_off = fd_ads_size;
			//fd_ads_size += buffer_size;
			beg_off = (*fd_size);
			(*fd_size) += buffer_size;
			mtx_fwt.unlock();
			
			assert((beg_off%512)==0);
			long bytes = pwrite(fd, mem_buffer, buffer_size, beg_off);
			assert(bytes==buffer_size);
			num_entry = 0;
		}
		mem_buffer[num_entry++] = tmp;
	}
};

//ads_entry make_ads_entry(unsigned vid, unsigned dist) {ads_entry e = {vid, dist}; return e;}
ads_entry make_ads_entry(unsigned src, unsigned dst, unsigned dist) {ads_entry u = {src, dst, dist}; return u;}
ads_update make_ads_update(unsigned src, unsigned dst, unsigned dist) {ads_update u = {src, dst, dist}; return u;}

int k = 4;
int num_part = 256;
unsigned NUM_THD = 16;
unsigned nodes_per_part;

int edge_type;
long num_nodes = 0;
long num_edges = 0;
//long* outdeg;
//long* indeg;
//long* out_off;
//long* in_off;
//unsigned* outedges;
//unsigned* inedges;

my_buffer* ads_buffer;
my_buffer* upd_buffer;

long** num_grid_edges;
unsigned*** grid_edges;

unsigned* ranks;

class ads_struct{ 
public:
	mutex ads_mtx;

	ads_entry* top_k_ads; // (top k smallest ads) maintained in max_heap structure
	unsigned num_ads;
	
	//vector<ads_entry> new_ads; // for simplicity, we assume new_ can be inside either top_ or old_
	ads_entry* new_ads;
	unsigned num_new_ads;
	
public:
	ads_struct(){
		num_ads = 0;
		num_new_ads = 0;
	}
	
	~ads_struct(){
		// do nothing
	}
	
	inline void cleanup(ads_entry input){ // how to optimize this
		// do nothing here if we remove the unique distances assumption for unweighted graphs
		// i.e., the vertex which arrive first will be treated as the closer one.
	}
	
	void all_cleanup(){ 
		// cleanup() via new_ads
	}
	
	bool insert(ads_entry input, unsigned tid){
		// should we tackle the mutex issue for baseline?
		// e.g., we can create NUM_THD buffers for each vertex partition ..
		// .. so that each thread can process its own buffer.
		// Then we will need (NUM_THD * num_part) buffers (aka files). Is that too many?
		// If the preprocessing procedure can be aware of this mutex-free purpose, ..
		// .. we can reduce # buffers to NUM_THD. (each thread process one (sub-)grid at a time).
		// However, this will freeze the # threads during runtime.

		if (num_ads < k){
			ads_mtx.lock();
			if (num_ads == k){
				ads_mtx.unlock();
				goto CHECK;
			}
			
			unsigned input_rank = ranks[input.src];
			top_k_ads[num_ads++] = input;
			int cur_idx = num_ads-1;
			bool check = true;
			while (cur_idx != 0 && check){
				int parent = (cur_idx-1) / 2;
				if (input_rank > ranks[top_k_ads[parent].src]){
					ads_entry tmp = top_k_ads[parent]; 
					top_k_ads[parent] = top_k_ads[cur_idx]; 
					top_k_ads[cur_idx] = tmp;
					
					cur_idx = parent;
					check = true;
				}
				else check = false;
			}
			
			new_ads[num_new_ads++] = input;
			ads_mtx.unlock();
			return true;
		}
		else{
			CHECK: 
			unsigned cur_biggest_rank = ranks[top_k_ads[0].src];
			unsigned input_rank = ranks[input.src];
			if (input_rank >= cur_biggest_rank){
				return false;
			}
			else {
				ads_mtx.lock();
				cur_biggest_rank = ranks[top_k_ads[0].src]; // update the current biggest rank
				if (input_rank >= cur_biggest_rank){
					ads_mtx.unlock();
					return false;
				}


				ads_buffer[tid].mutex_free_entry_write(fd_ads[tid], &fd_ads_size[tid], top_k_ads[0]); // append all old-ads to the file
				// ads_buffer[tid].entry_write(fd_ads, &fd_ads_size, top_k_ads[0]); // append all old-ads to the file
				
				
				top_k_ads[0] = input;
				int cur_idx = 0, chosen = 0;
				//while (cur_idx == chosen){
				while (true){
					int left = cur_idx * 2 + 1, right = cur_idx * 2 + 2;
					unsigned left_rank = 0, right_rank = 0;
					if (left < k) left_rank = ranks[top_k_ads[left].src];
					if (right < k) right_rank = ranks[top_k_ads[right].src];

					if (input_rank < left_rank){
						chosen = left;
						if (left_rank < right_rank)
							chosen = right;
					}
					if (cur_idx == chosen && input_rank < right_rank){
						chosen = right;
					}
					
					//if (left < k && input_rank < ranks[top_k_ads[left].src])
					//	chosen = left;
					//if (right < k && input_rank < ranks[top_k_ads[right].src])
					//	chosen = right;
					
					if (cur_idx != chosen){
						ads_entry tmp = top_k_ads[chosen]; 
						top_k_ads[chosen] = top_k_ads[cur_idx]; 
						top_k_ads[cur_idx] = tmp;
						cur_idx = chosen;
					}
					else{ // chosen == cur_idx -> choose nothing
						break;
					}
				}
				
				num_ads++;
				new_ads[num_new_ads++] = input;
				//cleanup(input); 
				ads_mtx.unlock();

				return true;
			}
		}
	}
	
	void empty_new_ads(){
		// new_ads.clear();
		num_new_ads = 0;
	}
};

inline int get_part_id (unsigned vid)
{
	unsigned a = vid/nodes_per_part;
	unsigned b = num_part-1;
	return (a < b)? a: b;
}


/////***** Start Initailize Global Variables *****/////
long* buff_sz;
long* next_buff_sz;
ads_update** buff;
ads_update** next_buff;

ads_struct* all_ads;

Queue<tuple<unsigned, unsigned, long, long> > tasks(65536);

long new_cnt = 0; // newly generated entires in an iteration
long total_cnt = 0; // # all entires in all ADS
/////***** End Initailize Global variables *****/////



void construct_graph(string input_path, string output_path)
{
	cout << "Start to read raw graph data\n";
	double beg_tm = wtime();
	
	FILE *fr;

	//outdeg = new long [num_nodes]{};
	//indeg = new long [num_nodes]{};
	num_edges = file_size(input_path) / (long)8;
	
	num_grid_edges = new long* [num_part];
	for(int pi = 0; pi < num_part; pi++){
		num_grid_edges[pi] = new long [num_part]{};
	}
	
	fr = fopen(input_path.c_str(), "rb");
	int src, dst;
	long cnt_edges = 0;
	while(true){
		long bytes = fread(&src, sizeof(unsigned), 1, fr);
		if (bytes == 0) break;
		fread(&dst, sizeof(unsigned), 1, fr);
		assert(bytes != 0);

		num_grid_edges[get_part_id(src)][get_part_id(dst)]++; // original graph 
		//num_grid_edges[get_part_id(dst)][get_part_id(src)]++; // transpose graph
		cnt_edges++;
	}
	assert(num_edges == cnt_edges);
	fclose(fr);
	
	grid_edges = new unsigned** [num_part];
	for(int px = 0; px < num_part; px++){
		grid_edges[px] = new unsigned* [num_part];
		for(int py = 0; py < num_part; py++){
			grid_edges[px][py] = new unsigned [2*num_grid_edges[px][py]]{};
			num_grid_edges[px][py] = 0; // for using this structure later
		}
	}
	cout << "... finish creating spaces for holding edge data\n";

	
	fr = fopen(input_path.c_str(), "rb");
	while(true){
		long bytes = fread(&src, sizeof(unsigned), 1, fr);
		if (bytes == 0) break;
		fread(&dst, sizeof(unsigned), 1, fr);
		assert(bytes != 0);
		
		//int tmp = src; src = dst; dst = tmp; // transpose graph 
		
		int src_pid = get_part_id(src);
		int dst_pid = get_part_id(dst);
		int aggre_edges = num_grid_edges[src_pid][dst_pid];
		
		grid_edges[src_pid][dst_pid][2*aggre_edges] = src;
		grid_edges[src_pid][dst_pid][2*aggre_edges+1] = dst;
		num_grid_edges[src_pid][dst_pid]++;
	}
	fclose(fr);
	cout << "... finish writing edge list\n";
	
	double end_tm = wtime();
	
	cout << "Reading raw graph data totally takes " << end_tm - beg_tm << " seconds\n\n";
	fflush(stdout);
}

void read_graph(string graph_path)
{
	// read graph metadata
	/*fstream fd_meta;
	fd_meta.open(graph_path+"metadata", ios::in);
	fd_meta >> num_nodes;
	fd_meta >> num_edges;
	fd_meta >> num_part;
	for (int r = 0; r < num_part; ++r){
		for (int c = 0; c < num_part; ++c){
			fd_meta >> num_grid_edges[r][c];
		}
	}
	fd_meta.close();*/
	
	// open graph files
	
	// create buffer files under graph_path
	
}

void receive_updates(unsigned tid)
{
	long local_new_cnt = 0;
	while(true){
		unsigned row_part, col_part;
		long cur_off, end_off;
		tie(row_part, col_part, cur_off, end_off) = tasks.pop();
		if (row_part ==  -1) break;
		
		unsigned row_beg_vid = row_part * nodes_per_part;
		unsigned row_end_vid = (row_part+1) * nodes_per_part;
		row_end_vid = (num_nodes < row_end_vid)? num_nodes: row_end_vid;
		
		upd_buffer[tid].chunk_read(fd_rd_upd[row_part], cur_off, end_off);
		long num_rd_entry = (end_off - cur_off) / sizeof(ads_entry);
		for(long idx = 0; idx < num_rd_entry; idx++){
			
			//if ((idx%2)==0) continue; // accelerate to see the correctness
			
			//ads_update & upd = *(ads_update*)(upd_buffer[tid].mem_buffer + ptr);
			
			ads_entry upd = upd_buffer[tid].mem_buffer[idx];
			bool success = all_ads[upd.dst-row_beg_vid].insert(upd, tid);
			if (success) local_new_cnt++;
		}
	}
	write_add(&new_cnt, local_new_cnt);
}

void produce_updates(unsigned tid) // should we tackle the mutex issue for baseline?
{
	while(true){
		unsigned row_part = 0, col_part = 0;
		long cur_idx, end_idx;
		tie(row_part, col_part, cur_idx, end_idx) = tasks.pop();
		if (row_part ==  -1) break;

		unsigned row_beg_vid = row_part * nodes_per_part;
		unsigned row_end_vid = (row_part+1) * nodes_per_part;
		row_end_vid = (num_nodes < row_end_vid)? num_nodes: row_end_vid;
		
		long beg_off = column_offset[col_part * num_part + row_part];
		long end_off = column_offset[col_part * num_part + row_part + 1];
		if (beg_off == end_off) continue;

		long cur_off = ((beg_off%512)!=0)? (beg_off/512)*512: beg_off;
		end_off = ((end_off%512)!=0)? (end_off/512+1)*512: end_off;
		while (end_off - cur_off > 0){
			long length = (end_off - cur_off >= batch_size)? batch_size: end_off - cur_off;
			assert(length%512==0);
			long bytes = pread(fd_edge, edge_buffer[tid], length, cur_off);
			//assert(length == bytes);
			cur_off += length;
			
			for(long ptr = 0; ptr < bytes; ptr+=8){
				
				unsigned src = *(unsigned*)(edge_buffer[tid] + ptr);
				if (row_beg_vid <= src && src < row_end_vid){
					unsigned dst = *(unsigned*)(edge_buffer[tid] + ptr + 4);
					unsigned weight = 1;
					
					unsigned src_off = src - row_beg_vid;
					for(unsigned i = 0; i < all_ads[src_off].num_new_ads; ++i){
						ads_entry tmp = all_ads[src_off].new_ads[i];
						ads_entry new_tmp = make_ads_entry(tmp.src, dst, tmp.dist+weight);
						upd_buffer[tid].mutex_free_entry_write(fd_wt_upd[col_part], &fd_wt_upd_size[col_part], new_tmp);
					}
				}
			}
		}
		upd_buffer[tid].flush();
	}
}

int main(int argc, char ** argv)
{
	string path = argv[1];
	k = atol(argv[2]);
	NUM_THD = atol(argv[3]);
	
	// read meta
	FILE * fin_meta = fopen((path+"meta").c_str(), "r");
	fscanf(fin_meta, "%d %ld %ld %d", &edge_type, &num_nodes, &num_edges, &num_part);
	fclose(fin_meta);
	cout << edge_type << " " << num_nodes << " " << num_edges << " " << num_part << endl;
	
	nodes_per_part = num_nodes / num_part;
	
	column_offset = new long [num_part*num_part+1];
	int fin_column_offset = open((path+"column_offset").c_str(), O_RDONLY);
	long bytes = read(fin_column_offset, column_offset, sizeof(long)*(num_part*num_part+1));
	assert(bytes==sizeof(long)*(num_part*num_part+1));
	close(fin_column_offset);


	fd_edge = open((path+"column").c_str(), O_RDONLY | O_DIRECT); // edge_buffer
	if (fd_edge == -1){
		cout << "edge file open error!" << endl;
		assert(0 == 1);
	}

	fd_rd_upd = new int [num_part]{}; // upd_buffer
	fd_wt_upd = new int [num_part]{}; // upd_buffer
	fd_top_k_ads = new int [num_part]{}; // buffer_top_k_ads
	for(int i = 0; i < num_part; ++i){
		string pid_str = to_string(i);
		fd_rd_upd[i] = open(((path+"cur_iter_upd.p")+pid_str).c_str(), O_RDWR | O_CREAT | O_TRUNC | O_DIRECT);
		fd_wt_upd[i] = open(((path+"nxt_iter_upd.p")+pid_str).c_str(), O_RDWR | O_CREAT | O_TRUNC | O_DIRECT);
		fd_top_k_ads[i] = open(((path+"top_k_ads.p")+pid_str).c_str(), O_RDWR | O_CREAT | O_TRUNC | O_DIRECT);
		if (fd_rd_upd[i] == -1 || fd_wt_upd[i] == -1 || fd_top_k_ads[i] == -1){
			cout << "file open error!" << endl;
			assert(0 == 1);
		}
	}
	fd_rd_upd_size = new long [num_part]{};
	fd_wt_upd_size = new long [num_part]{};

	fd_ads = new int [NUM_THD];
	for (int tid = 0; tid < NUM_THD; ++tid){
		string tid_str = to_string(tid);
		fd_ads[tid] = open((path+"all_ads.t"+tid_str).c_str(), O_RDWR | O_CREAT | O_TRUNC | O_DIRECT); // ads_buffer
		if (fd_ads[tid] == -1){
			cout << "ads file open error!" << endl;
			assert(0 == 1);
		}
	}
	fd_ads_size = new long [NUM_THD]{};
	/*fd_ads = open((path+"all_ads").c_str(), O_RDWR | O_CREAT | O_TRUNC | O_DIRECT); // ads_buffer
	if (fd_ads[tid] == -1){
		cout << "ads file open error!" << endl;
		assert(0 == 1);
	}
	fd_ads_size = 0;*/


	ads_buffer = new my_buffer [NUM_THD];
	upd_buffer = new my_buffer [NUM_THD];
	edge_buffer = new char* [NUM_THD];
	ads_buffer_pool = (char*)memalign(4096, NUM_THD * buffer_size);
	upd_buffer_pool = (char*)memalign(4096, NUM_THD * buffer_size);
	edge_buffer_pool = (char*)memalign(4096, NUM_THD * buffer_size);
	assert(ads_buffer_pool != NULL);
	assert(upd_buffer_pool != NULL);
	assert(edge_buffer_pool != NULL);
	for (int tid = 0; tid < NUM_THD; ++tid){
		ads_buffer[tid].mem_buffer = (ads_entry*)(ads_buffer_pool + tid * buffer_size);
		upd_buffer[tid].mem_buffer = (ads_entry*)(upd_buffer_pool + tid * buffer_size);
		edge_buffer[tid] = (char*)(edge_buffer_pool + tid * buffer_size);
	}
	assert((buffer_size%sizeof(ads_entry)) == 0);
	assert((buffer_size%512) == 0);

	//// Start to create data structures for baseline ADS construction

	//construct_graph(path, "NULL"); // if the input path points to the raw graph data
	//read_graph(path); // if the input path points to the grid-based format
	
	// Three data structures for ADS (old enties, newly-added entries, and top-k entries)
	all_ads = new ads_struct [nodes_per_part];
	
	top_k_total_sz = sizeof(ads_entry) * nodes_per_part * (long)k;
	top_k_total_sz = ((top_k_total_sz%512)!=0)? (top_k_total_sz/512+1)*512: top_k_total_sz;
	buffer_top_k_ads = (char*)memalign(4096, top_k_total_sz);
	assert(buffer_top_k_ads != NULL);
	for(long vid = 0; vid < nodes_per_part; ++vid){
		all_ads[vid].top_k_ads = (ads_entry*)(buffer_top_k_ads + vid * k * sizeof(ads_entry));
	}

	long tmp = k * (long)(log2(num_nodes)+10); // klogV
	//long tmp = k * 20; // klogV
	new_ads_total_sz = sizeof(ads_entry) * nodes_per_part * tmp;
	new_ads_total_sz = ((new_ads_total_sz%512)!=0)? (new_ads_total_sz/512+1)*512: new_ads_total_sz;
	buffer_new_ads = (char*)memalign(4096, new_ads_total_sz);
	assert(buffer_new_ads != NULL);
	for(long vid = 0; vid < nodes_per_part; ++vid){
		all_ads[vid].new_ads = (ads_entry*)(buffer_new_ads + vid * tmp * sizeof(ads_entry));
		all_ads[vid].num_new_ads = 0;
	}


	// Randomly assigning rank value 
	ranks = new unsigned [num_nodes]{};
	for(unsigned vid = 0; vid < num_nodes; ++vid)
		ranks[vid] = vid+1;
	srand(0); // -> srand(time(0));
	random_device rd;
	mt19937 g(rd());
	shuffle(ranks, ranks+num_nodes, g); // rank value tie-breaking
	
	// Baseline ADS construction
	int cur_iter = 0;
	/*for(unsigned vid = 0; vid < num_nodes; ++vid){ // initialization
		int part_id = get_part_id(vid);
		buff[part_id][buff_sz[part_id]++] = make_ads_update(vid, vid, 0);
	}*/
	
	cout << "Start ADS Construction" << endl;
	fflush(stdout);
	
	threads = new thread [NUM_THD]{};
	
	double beg_time = wtime();
	while(cur_iter == 0 || new_cnt > 0) {
		new_cnt = 0;
		
		double iter_beg_time = wtime();
		for(unsigned row_pid = 0; row_pid < num_part; ++row_pid){ // two phases for processing a partition
			unsigned row_beg_vid = row_pid * nodes_per_part;
			unsigned row_end_vid = (row_pid+1) * nodes_per_part;
			row_end_vid = (num_nodes < row_end_vid)? num_nodes: row_end_vid;
			
			//cout << "begin of phase 1, " << row_pid << endl;
			//fflush(stdout);
			
			//** phase 1, receive info of buff[row_pid] & try to update ads@row_pid **//
			if (cur_iter > 0){
				long bytes = pread(fd_top_k_ads[row_pid], buffer_top_k_ads, top_k_total_sz, 0); // fill in top_k_ads first
				//assert(bytes == top_k_total_sz);

				for(unsigned tid = 0; tid < NUM_THD; ++tid)
					threads[tid] = thread(&receive_updates, tid);
				
				//long cur_off = 0, end_off = buff_sz[row_pid];
				long cur_off = 0, end_off = fd_rd_upd_size[row_pid];
				while (cur_off < end_off){
					long tmp_off = cur_off + batch_size;
					tmp_off = (tmp_off < end_off)? tmp_off: end_off;
					tasks.push(make_tuple(row_pid, 0, cur_off, tmp_off));
					cur_off = tmp_off;
				}
				for(unsigned tid = 0; tid < NUM_THD; ++tid)
					tasks.push(make_tuple(-1, -1, 0, 0));
				
				for(unsigned tid = 0; tid < NUM_THD; ++tid)
					threads[tid].join();
				fd_rd_upd_size[row_pid] = 0;
				//buff_sz[row_pid] = 0;
				assert(tasks.is_empty());

				bytes = pwrite(fd_top_k_ads[row_pid], buffer_top_k_ads, top_k_total_sz, 0); // persist top_k_ads
				//assert(bytes == top_k_total_sz);

				//cout << "end of phase 1, " << row_pid << endl;
			}
			

			//** phase 2, based on the newly-added ads@row_pid, generate info to other buffers **//
			//cout << "begin of phase 2, " << row_pid << endl;
			//fflush(stdout);
			if (cur_iter == 0){
				for(long cur_vid = 0; cur_vid < nodes_per_part; ++cur_vid){ // initialization
					long real_vid = cur_vid + row_beg_vid;
					//cout << nodes_per_part << " " << real_vid << endl;
					//fflush(stdout);
					all_ads[cur_vid].new_ads[0] = make_ads_entry(real_vid, real_vid, 0);
					++all_ads[cur_vid].num_new_ads;
				}
			}
			//cout << "2-a" << endl;
			//fflush(stdout);
			
			for(unsigned tid = 0; tid < NUM_THD; ++tid)
				threads[tid] = thread(&produce_updates, tid);
			
			//cout << "2-b" << endl;
			//fflush(stdout);
			
			for(unsigned col_pid = 0; col_pid < num_part; col_pid++)
				tasks.push(make_tuple(row_pid, col_pid, 0, 0));
			for(unsigned tid = 0; tid < NUM_THD; ++tid)
				tasks.push(make_tuple(-1, -1, 0, 0));
			
			//cout << "2-c" << endl;
			//fflush(stdout);
			
			for(unsigned tid = 0; tid < NUM_THD; ++tid)
				threads[tid].join();
			for(long cur_vid = 0; cur_vid < nodes_per_part; ++cur_vid)
				all_ads[cur_vid].num_new_ads = 0;
			
			//cout << "2-d" << endl;
			//fflush(stdout);
			
			/*if (tasks.is_empty() == false){
				while(!tasks.is_empty()){
					unsigned row_part, col_part;
					long cur_idx, end_idx;
					tie(row_part, col_part, cur_idx, end_idx) = tasks.pop();
					cout << row_part << " " << col_part << " " << cur_idx << " " << end_idx << endl;
				}
			}*/
			assert(tasks.is_empty());
			
			//cout << "end of phase 2, " << row_pid << endl;
			//fflush(stdout);
		}
		double iter_end_time = wtime();
		if (cur_iter == 0) new_cnt = num_nodes;
		
		
		//swap(next_buff, buff);
		//swap(next_buff_sz, buff_sz);
		swap(fd_rd_upd, fd_wt_upd);
		swap(fd_rd_upd_size, fd_wt_upd_size);
		printf("%d: %lld %lld, takes time: %.2f\n", ++cur_iter, new_cnt, total_cnt, iter_end_time-iter_beg_time);
		fflush(stdout);
		total_cnt += new_cnt;
	}
	// top_k_ads + all_ads = complete ads

	double end_time = wtime();
	cout << "Total Time = " << end_time - beg_time << ", " << total_cnt << endl;

	delete [] threads;
	delete [] all_ads;
	return 0;
}
