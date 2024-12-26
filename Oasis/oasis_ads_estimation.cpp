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

#define debugmode1 (0) // io debugging
#define debugmode2 (0) // ads debugging

using namespace std;

long max_ele_per_buff = (long)32*1024*1024;

mutex* buff_mtx;
thread* threads;

int** fd_rd_upd;
long** fd_rd_upd_size;

int** fd_wt_upd;
long** fd_wt_upd_size;

int* fd_ads;
long* fd_ads_size;
//int fd_ads;
//long fd_ads_size;

long* column_offset;
int fd_edge;

int* fd_top_k_ads;

mutex mtx_fwt;
int my_lock1 = 0;
int my_lock2 = 0;

long klogV;
long VklogV;

int cur_iter = 0;

char* new_ads_pool;
char* ads_buffer_pool;
char* upd_buffer_pool;
char* edge_buffer_pool;
long buffer_size = 3 * 1024 * 1024;
long batch_size = 3 * 1024 * 1024;

char* buffer_top_k_ads;
long top_k_total_sz;

char* buffer_new_ads;
long new_ads_total_sz;

template <class ET>
inline ET up_aligned(ET input, auto align){
	if ((input%align) == 0)
		return input;
	else return ((input/align)+1) * align;
}

double wtime(){
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
	unsigned root_vid;
	unsigned dist;
};

struct ads_update{
	unsigned src, dst;
	unsigned dist;
};

struct idx_pair{
	long beg_idx;
	long end_idx;
};

//ads_entry make_ads_entry(unsigned vid, unsigned dist) {ads_entry e = {vid, dist}; return e;}
ads_entry make_ads_entry(unsigned root_vid, unsigned dist) {ads_entry u = {root_vid, dist}; return u;}
ads_update make_ads_update(unsigned src, unsigned dst, unsigned dist) {ads_update u = {src, dst, dist}; return u;}

int k;
int num_part;
unsigned NUM_THD;
unsigned part_num_nodes;
int num_grids;

unsigned* ranks;

int edge_type;
long num_nodes = 0;
long num_edges = 0;
//long* outdeg;
//long* indeg;
//long* out_off;
//long* in_off;
//unsigned* outedges;
//unsigned* inedges;

char** edge_buffer;
ads_entry** upd_buffer;
ads_entry* ads_buffer;
ads_entry* new_ads_buffer;
ads_entry* new_ads_buffer_1;
ads_entry* new_ads_buffer_2;

ads_entry* sampled_cannot_ads; // for skipping ads-list traversal

long** num_grid_edges;
unsigned*** grid_edges;
idx_pair* ads_off;
idx_pair* new_ads_off;
long* idx_new_ads_buffer;

unsigned** vtx_state;
unsigned** bfs_active_vid; // for ads_update merging

bitmap active_src_vid;
bitmap exist_src_vid;

long one_gb = 1024 * 1024 * 1024;
long my_fwrite(int fd, void* ads_buffer, long length, long beg_off)
{
	if (length > one_gb){
		long bytes = 0;
		while(bytes < length){
			if (bytes + one_gb <= length){
				long ret_bytes = pwrite(fd, ads_buffer, one_gb, beg_off);
				assert(ret_bytes==one_gb);
				bytes += ret_bytes;
				beg_off += ret_bytes;
			}
			else{
				long new_length = length - bytes;
				assert(new_length < one_gb);
				long ret_bytes = pwrite(fd, ads_buffer, new_length, beg_off);
				assert(ret_bytes==new_length);
				bytes += ret_bytes;
				beg_off += ret_bytes;
			}
		}
		assert(bytes==length);
		return bytes;
	}
	else{
		return pwrite(fd, ads_buffer, length, beg_off);
	}
}

long my_fread(int fd, void* ads_buffer, long length, long beg_off)
{
	if (length > one_gb){
		long bytes = 0;
		while(bytes < length){
			if (bytes + one_gb <= length){
				long ret_bytes = pread(fd, ads_buffer, one_gb, beg_off);
				assert(ret_bytes==one_gb);
				bytes += ret_bytes;
				beg_off += ret_bytes;
			}
			else{
				long new_length = length - bytes;
				assert(new_length < one_gb);
				long ret_bytes = pread(fd, ads_buffer, new_length, beg_off);
				assert(ret_bytes==new_length);
				bytes += ret_bytes;
				beg_off += ret_bytes;
			}
		}
		assert(bytes==length);
		return bytes;
	}
	else{
		return pread(fd, ads_buffer, length, beg_off);
	}
}


int* cur_fd_active_ads;
int* nxt_fd_active_ads;
long** cur_active_ads_idx;
long** nxt_active_ads_idx;
long** all_ads_cnt;
long** block_idx; // each thread is provided with an index data 

//////////////////////////////////////////////////////////////////////
unsigned max_ctx;
unsigned max_event;
io_context_t* ctx;
struct iocb** io_cbp;
struct iocb*** piocb;
struct io_event** events;
void aio_init()
{
	// aio initialization
	/// https://developer.aliyun.com/article/461437: async io example
	max_ctx = NUM_THD; // NUM_THD
	max_event = 64; // random guess
	ctx = new io_context_t [max_ctx]{};
	//memset(&ctx, 0, sizeof(ctx));
	for(unsigned i = 0;i < max_ctx; i++){
		int errcode = io_setup(max_event, &ctx[i]);
		if (errcode == 0)
			cout << "io_setup success\n";
		else
			cout << "io_setup error\n";
	}
	io_cbp = new struct iocb* [max_ctx];
	for(unsigned i = 0; i < max_ctx; i++)
		io_cbp[i] = new struct iocb [max_event];
	
	events = new struct io_event* [max_ctx];
	for(int i = 0; i < max_ctx; i++)
		events[i] = new struct io_event [max_event];
	
	piocb = new struct iocb** [max_ctx];
	for(int i = 0; i < max_ctx; i++)
		piocb[i] = new struct iocb* [max_event];
	
	//events = new struct io_event [max_event];
	//piocb = new struct iocb* [max_event];
}
unsigned aio_read(unsigned tid, long* req_pd_id, unsigned num_req, void* buffer, long buff_sz, unsigned part_id)
{
	if (num_req == 0) return 0;
	assert(num_req != 0);

	int read_unit = 4096; // 4KB
	long numRead = 0;
	unsigned page_cnt = 0;

	unsigned req_cnt = 0;
	unsigned cur_req_sz = 0;
	unsigned beg_req_pd_id, last_pd_id;
	for(unsigned i = 0; i < num_req; ++i){
		/*if (cur_req_sz == 0){
			beg_req_pd_id = req_pd_id[i];
			last_pd_id = req_pd_id[i];
			cur_req_sz = read_unit;
		}
		else if (last_pd_id+1 == req_pd_id[i]){
			++last_pd_id; //last_pd_id = req_pd_id[i];
			cur_req_sz += read_unit;
		}
		else{
			piocb[tid][req_cnt] = &io_cbp[tid][req_cnt];
			io_prep_pread(&io_cbp[tid][req_cnt], cur_fd_active_ads[part_id], buffer+numRead, cur_req_sz, beg_req_pd_id * (long)4096);
			numRead += cur_req_sz;
			cur_req_sz = 0;
			--i;
			req_cnt++;
		}*/
		
		piocb[tid][req_cnt] = &io_cbp[tid][req_cnt];
		io_prep_pread(&io_cbp[tid][req_cnt], cur_fd_active_ads[part_id], buffer+req_pd_id[i]*read_unit, read_unit, req_pd_id[i] * (long)4096);
		numRead += read_unit;
		req_cnt++;

		if (req_cnt == max_event){
			int ret = io_submit(ctx[tid], req_cnt, piocb[tid]);
			if(ret != req_cnt){
				cout << "io_submit error" << endl; exit(-1);
			}
			ret = io_getevents(ctx[tid], req_cnt, max_event, events[tid], NULL);
			if (ret != req_cnt){
				cout << "io_getevents error" << endl; exit(-1);
			}
			req_cnt = 0;
			assert(cur_req_sz == 0);
		}
	}

	/*if (cur_req_sz > 0){
		piocb[tid][req_cnt] = &io_cbp[tid][req_cnt];
		io_prep_pread(&io_cbp[tid][req_cnt], cur_fd_active_ads[part_id], buffer+numRead, cur_req_sz, beg_req_pd_id * (long)4096);
		numRead += cur_req_sz;
		cur_req_sz = 0;
		req_cnt++;
	}*/
	if (req_cnt > 0){
		int ret = io_submit(ctx[tid], req_cnt, piocb[tid]);
		if(ret != req_cnt){
			cout << "io_submit error" << endl; exit(-1);
		}
		ret = io_getevents(ctx[tid], req_cnt, max_event, events[tid], NULL);
		if (ret != req_cnt){
			cout << "io_getevents error" << endl; exit(-1);
		}
		req_cnt = 0;
	}
	assert(numRead <= buff_sz);
	//cout << numRead << " " << numRead / 4096 << " "  << num_req << endl;
	return numRead / 4096;

}
//////////////////////////////////////////////////////////////////////


inline int get_part_id (unsigned vid)
{
	unsigned a = vid/part_num_nodes;
	unsigned b = num_part-1;
	return (a < b)? a: b;
}

/////***** Start Initailize Global Variables *****/////
long* buff_sz;
long* next_buff_sz;
ads_update** buff;
ads_update** next_buff;

Queue<tuple<unsigned, unsigned, long, long> > tasks(65536); // grid_id, block_id, beg_off, end_off

long new_cnt = 0; // newly generated entires in an iteration
long total_cnt = 0; // # all entires in all ADS
/////***** End Initailize Global variables *****/////

long fileSize(int fd) {
   struct stat s;
   if (fstat(fd, &s) == -1) {
      int saveErrno = errno;
      fprintf(stderr, "fstat(%d) returned errno=%d.", fd, saveErrno);
      return(-1);
   }
   return(s.st_size);
}

// Graph: social-LiveJournal
// sudo ./a.out 1 /data/nvme_raid/yangty/ADS/soc_column /data/nvme_raid/yangty/ADS/soc-LiveJournal.2p.grid_block/ 4847616 2   (136, 14, 14, 14)
// sudo ./a.out 1 /data/nvme_raid/yangty/ADS/soc_column /data/nvme_raid/yangty/ADS/soc-LiveJournal.16p.grid_block/ 4847616 16
// sudo ./a.out 2 /data/nvme_raid/yangty/ADS/soc-LiveJournal.2p.grid_block/ 1 16

// Graph: Twitter
// sudo ./a.out 1 /data/nvme_raid/yangty/ADS/tw_column /data/nvme_raid/yangty/ADS/twitter.64p.grid_block/ 41652544 64
// sudo ./a.out 1 /data/nvme_raid/yangty/ADS/tw_column /data/nvme_raid/yangty/ADS/twitter.32p.grid_block/ 41652544 32
// sudo ./a.out 1 /data/nvme_raid/yangty/ADS/tw_column /data/nvme_raid/yangty/ADS/twitter.16p.grid_block/ 41652544 16
// sudo ./a.out 2 /data/nvme_raid/yangty/ADS/twitter.16p.grid_block/ 4 8

long* req_pg_id;
int main(int argc, char ** argv)
{
	NUM_THD = 16;
	
	/*int model_selection = atol(argv[1]);
	if (model_selection == 1){ // do preprocessing
		string graph_path = argv[2];
		string output_path = argv[3];
		num_nodes = atol(argv[4]);
		num_part = atol(argv[5]);
		int apply_random_order = atol(argv[6]);

		ranks = new unsigned [num_nodes]{};
		for(unsigned vid = 0; vid < num_nodes; ++vid)
			ranks[vid] = vid;
		if (apply_random_order == 1){ // Randomly assigning rank value 
			srand(0); // -> srand(time(0));
			random_device rd;
			mt19937 g(rd());
			shuffle(ranks, ranks+num_nodes, g); // rank value tie-breaking
		}
		else assert(apply_random_order==0);
		
		assert(apply_random_order==1);
		graph_preprocessing(graph_path, output_path);
		
		delete [] ranks;
		return 0;
	}
	assert(model_selection == 2); // do ads_construction
	string path = argv[2];
	k = atol(argv[3]);
	NUM_THD = atol(argv[4]);
	int use_rank_reordering = atol(argv[5]);*/

	// read meta
	FILE * fin_meta = fopen((path+"meta").c_str(), "r");
	fscanf(fin_meta, "%d %ld %ld %d", &edge_type, &num_nodes, &num_edges, &num_part);
	fclose(fin_meta);
	cout << path << ", K = " << k << ", NUM_THD = " << NUM_THD << endl;
	cout << edge_type << " " << num_nodes << " " << num_edges << " " << num_part << endl;
	
	part_num_nodes = num_nodes / num_part;
	num_grids = num_part * num_part;
	
	column_offset = new long [num_grids+1];
	int fin_column_offset = open((path+"column_offset").c_str(), O_RDONLY);
	long bytes = read(fin_column_offset, column_offset, sizeof(long)*(num_grids+1));
	assert(bytes==sizeof(long)*(num_grids+1));
	close(fin_column_offset);



	klogV = k * (long)(log2(num_nodes)+2); 
	VklogV = num_nodes * klogV;

	long* ads_idx = new long [num_nodes+1]{};
	ads_entry* ads_list = new ads_entry [VklogV]{};
	unsigned* attr = new unsigned [num_nodes]{};
	unsigned* ans = new unsigned [num_nodes]{};

	// testing initialization
	int cnt = 1;
	for(unsigned vid = 0; vid < num_nodes; ++vid){
		attr[vid] = cnt++;
		if (cnt == 10000)
			cnt = 1;
	}
	for(long i = 1; i <= num_nodes; ++i){
		ads_idx[i] = i * klogV;
	}
	for(long i = 0; i < VklogV; ++i){
		ads_list[i].root_vid = (rand()%num_nodes);
		ads_list[i].dist = (rand()%255)+1;
	}
	
	// estimating centrality for all vertices
	double beg_time = wtime();
	vector<thread> threads;
	threads.clear();
	for(int tid = 0; tid < NUM_THD; ++tid){
		threads.emplace_back([&](int thread_id){
			for(unsigned vid = thread_id; vid < num_nodes; vid+=NUM_THD){
				long beg_idx = ads_idx[vid];
				long end_idx = ads_idx[vid+1];
				while(beg_idx < end_idx){
					ans[vid] += (attr[ads_list[beg_idx].root_vid] * ads_list[beg_idx].dist);
					++beg_idx;
				}
			}
		}, tid);
	}
	double end_time = wtime();
	
	


	edge_buffer = new char* [NUM_THD];
	
	new_ads_pool = (char*)memalign(4096, io_part_ads_sz);
	ads_buffer_pool = (char*)memalign(4096, io_part_ads_sz);
	edge_buffer_pool = (char*)memalign(4096, NUM_THD * buffer_size);
	assert(new_ads_pool != NULL);
	assert(ads_buffer_pool != NULL);
	assert(edge_buffer_pool != NULL);
	
	new_ads_buffer = (ads_entry*)new_ads_pool;
	new_ads_buffer_1 = (ads_entry*)new_ads_pool;
	new_ads_buffer_2 = (ads_entry*)(new_ads_pool + io_part_ads_sz/2);
	ready_buffer = -1;
	
	sampled_cannot_ads = new ads_entry [num_nodes]{};
	for(unsigned vid = 0; vid < num_nodes; ++vid){
		sampled_cannot_ads[vid].root_vid = -1;
		sampled_cannot_ads[vid].dist = -1;
	}
	
	for (int tid = 0; tid < NUM_THD; ++tid){
		edge_buffer[tid] = (char*)(edge_buffer_pool + tid * buffer_size);
	}
	assert((buffer_size%sizeof(ads_entry)) == 0);
	assert((buffer_size%512) == 0);

	ads_buffer = (ads_entry*)ads_buffer_pool;
	for (int part_id = 0; part_id < num_part; ++part_id){
		long bytes = my_fwrite(fd_ads[part_id], ads_buffer, io_part_ads_sz, 0);
		if (debugmode1){
			if (bytes != io_part_ads_sz){
				printf("Unexpected IO error in init fd_ads:\n");
				printf("Return bytes: %ld, expected return bytes: %ld\n", bytes, io_part_ads_sz);
			}
			assert(bytes==io_part_ads_sz);
		}
	}
	
	active_src_vid.init(num_nodes+32);
	exist_src_vid.init(num_nodes+32);
	
	block_idx = new long* [NUM_THD];
	for(int tid = 0; tid < NUM_THD; ++tid){
		block_idx[tid] = new long [part_num_nodes+1]{};
	}

	aio_init();
	req_pg_id = new long [io_part_ads_sz/4096+16]{};
	
	// Randomly assigning rank value	
	ranks = new unsigned [num_nodes]{};
	if (use_rank_reordering == 1){
		for(unsigned vid = 0; vid < num_nodes; ++vid)
			ranks[vid] = vid;
	}
	else if (use_rank_reordering == 0){
		srand(0); // -> srand(time(0));
		random_device rd;
		mt19937 g(rd());
		shuffle(ranks, ranks+num_nodes, g); // rank value tie-breaking
		/*int fin_ranks = open((path+"ranks").c_str(), O_RDONLY);
		long bytes = read(fin_ranks, ranks, sizeof(unsigned)*num_nodes);
		assert(bytes == sizeof(unsigned)*num_nodes);
		close(fin_ranks);*/
	}
	
	// Begin initializing
	cout << "Start ADS Construction (begin initialization)" << endl;
	fflush(stdout);
	
	double beg_time = wtime();
	for(int pid = 0; pid < num_part; ++pid){
		unsigned beg_vid = pid * part_num_nodes;
		unsigned end_vid = (pid+1) * part_num_nodes;
		end_vid = (num_nodes < end_vid)? num_nodes: end_vid;
		
		long idx = 0;
		unsigned cur_vid = beg_vid;
		while(cur_vid < end_vid){ // real initialization
			new_ads_buffer[idx] = make_ads_entry(cur_vid, 0);
			cur_active_ads_idx[pid][cur_vid-beg_vid] = idx;
			++idx;
			
			active_src_vid.set_bit(cur_vid);
			++cur_vid;
		}
		cur_active_ads_idx[pid][end_vid-beg_vid] = idx;
		
		//long io_size = ((idx * sizeof(ads_entry)) / 512 + 1) * 512;
		long io_size = up_aligned((idx * sizeof(ads_entry)), 512);
		long bytes = my_fwrite(cur_fd_active_ads[pid], new_ads_buffer, io_size, 0); // write active elements
		assert(bytes == io_size);
	}
	new_cnt = num_nodes;
	cur_iter = 0;
	threads = new thread [NUM_THD]{};
	cout << "End initialization" << endl;
	printf("%d: %lld %lld, takes time: %.2f (%.2f + %.2f)\n", cur_iter, new_cnt, total_cnt, wtime()-beg_time);
	fflush(stdout);
	// End initializing
	
	while(cur_iter == 0 || new_cnt > 0) {
		new_cnt = 0;
		
		double iter_beg_time = wtime();
		for(int dst_pid = 0; dst_pid < num_part; ++dst_pid){
			unsigned beg_dst_vid = dst_pid * part_num_nodes, end_dst_vid = (dst_pid+1) * part_num_nodes;
			end_dst_vid = (num_nodes < end_dst_vid)? num_nodes: end_dst_vid;
			
			//** Enabling Selective Access **//
			if (true){
				// Find out the src_vid with grid-edges
				exist_src_vid.clear();
				for(int tid = 0; tid < NUM_THD; ++tid)
					threads[tid] = thread(&compute_edge, tid);
				for(int src_pid = 0; src_pid < num_part; ++src_pid){ // assign tasks
					int grid_id = dst_pid * num_part + src_pid;
					int num_blocks;
					long grid_size = (column_offset[grid_id+1] - column_offset[grid_id]);
					if ((grid_size%buffer_size) == 0) num_blocks = grid_size / buffer_size;
					else num_blocks = grid_size / buffer_size + 1;
					//int num_blocks = (column_offset[grid_id+1] - column_offset[grid_id]) / buffer_size;
					for(int block_id = 0; block_id < num_blocks; ++block_id)
						tasks.push(make_tuple(grid_id, block_id, 0, 0));
				}
				for(int tid = 0; tid < NUM_THD; ++tid)
					tasks.push(make_tuple(-1, 0, 0, 0));
				
				long bytes, io_size;
				//// Read from storage to ads_buffer ////
				io_size = io_part_ads_sz;
				bytes = my_fread(fd_ads[dst_pid], ads_buffer, io_size, 0);
				if (debugmode1){
					if (bytes != io_size){
						printf("Unexpected IO error in reading fd_ads into ads_buffer:\n");
						printf("Return bytes: %ld, expected return bytes: %ld\n", bytes, io_size);
					}
				}
				assert(bytes == io_size);
				
				for(int tid = 0; tid < NUM_THD; ++tid)
					threads[tid].join();
				assert(tasks.is_empty());
				
				// Intersect two bitmaps: active_src_vid & exist_src_vid
				for(unsigned i = 0; i < active_src_vid.num_byte; ++i){
					//active_src_vid.data[i] &=  exist_src_vid.data[i];
					exist_src_vid.data[i] &= active_src_vid.data[i];
				}
			}
			else{
				for(unsigned i = 0; i < active_src_vid.num_byte; ++i){
					exist_src_vid.data[i] = 255;
				}
			}
			
			// Selectively load src_pid 0
			unsigned beg_src_vid = 0;
			unsigned end_src_vid = part_num_nodes;
			end_src_vid = (num_nodes < end_src_vid)? num_nodes: end_src_vid;
			unsigned num_pg = 0;
			for(unsigned cur_src_vid = beg_src_vid; cur_src_vid < end_src_vid; ++cur_src_vid){
				if (exist_src_vid.get_bit(cur_src_vid) == true){
					unsigned bias_vid =  cur_src_vid - beg_src_vid;
					long beg_off = cur_active_ads_idx[0][bias_vid];
					long end_off = cur_active_ads_idx[0][bias_vid+1];
					
					long beg_pg_id = beg_off / 4096;
					long end_pg_id = (end_off-1) / 4096;
					while(beg_pg_id <= end_pg_id){
						if (num_pg == 0){
							req_pg_id[num_pg++] = beg_pg_id;
						}
						else{
							if (req_pg_id[num_pg-1] != beg_pg_id){
								req_pg_id[num_pg++] = beg_pg_id;
							}
						}
						++beg_pg_id;
					}
				}
			}
			unsigned num_issued = aio_read(0, req_pg_id, num_pg, new_ads_buffer_1, io_part_ads_sz/2, 0); // Re-write new_ads_buffer
			assert(num_issued == num_pg);
			nxt_ready_buffer = 1;

			for(int src_pid = 0; src_pid < num_part; ++src_pid){ // Parallelly perform I/O and computation				

				ready_buffer = nxt_ready_buffer;
				
				//** Pipeline: Compute the grid of (src_pid, dst_pid) **//
				// use multi-threads to process ADS construction 
				for(int tid = 0; tid < NUM_THD; ++tid) // issuing computational-threads
					threads[tid] = thread(&compute_ads, tid);

				int grid_id = dst_pid * num_part + src_pid;
				int num_blocks;
				long grid_size = (column_offset[grid_id+1] - column_offset[grid_id]);
				if ((grid_size%buffer_size) == 0) num_blocks = grid_size / buffer_size;
				else num_blocks = grid_size / buffer_size + 1;
				for(int block_id = 0; block_id < num_blocks; ++block_id)
					tasks.push(make_tuple(grid_id, block_id, 0, 0));
				for(int tid = 0; tid < NUM_THD; ++tid)
					tasks.push(make_tuple(-1, 0, 0, 0));


				//** Pipeline: I/O on (src_pid+1) **//
				// selectively load the active ADS-elements based on the set-bits of active_src_vid (src_pid+1)
				int nxt_src_pid = src_pid+1;
				if (nxt_src_pid < num_part){
					unsigned beg_src_vid = nxt_src_pid * part_num_nodes;
					unsigned end_src_vid = (nxt_src_pid+1) * part_num_nodes;
					end_src_vid = (num_nodes < end_src_vid)? num_nodes: end_src_vid;
					unsigned num_pg = 0;
					for(unsigned cur_src_vid = beg_src_vid; cur_src_vid < end_src_vid; ++cur_src_vid){
						if (exist_src_vid.get_bit(cur_src_vid) == true){
							unsigned bias_vid =  cur_src_vid - beg_src_vid;
							long beg_off = cur_active_ads_idx[nxt_src_pid][bias_vid];
							long end_off = cur_active_ads_idx[nxt_src_pid][bias_vid+1];
							
							long beg_pg_id = beg_off / 4096;
							long end_pg_id = (end_off-1) / 4096;
							while(beg_pg_id <= end_pg_id){
								if (num_pg == 0){
									req_pg_id[num_pg++] = beg_pg_id;
								}
								else{
									if (req_pg_id[num_pg-1] != beg_pg_id){
										req_pg_id[num_pg++] = beg_pg_id;
									}
								}
								++beg_pg_id;
							}
						}
					}
					
					unsigned num_issued;
					if (ready_buffer == 1){
						num_issued = aio_read(0, req_pg_id, num_pg, new_ads_buffer_2, io_part_ads_sz/2, nxt_src_pid);
						nxt_ready_buffer = 2;
					}
					else if (ready_buffer == 2){
						num_issued = aio_read(0, req_pg_id, num_pg, new_ads_buffer_1, io_part_ads_sz/2, nxt_src_pid);
						nxt_ready_buffer = 1;
					}
					else{
						bool wrong_ready_buffer_val = false;
						assert(wrong_ready_buffer_val);
					}
					assert(num_issued == num_pg);
				}
				
				for(int tid = 0; tid < NUM_THD; ++tid)
					threads[tid].join();
				assert(tasks.is_empty());
			}

			
			//// Write ads_buffer to storage ////
			long io_size = io_part_ads_sz;
			bytes = my_fwrite(fd_ads[dst_pid], ads_buffer, io_size, 0);
			if (debugmode1){
				if (bytes != io_size){
					printf("Unexpected IO error in writing ads_buffer to fd_ads:\n");
					printf("Return bytes: %ld, expected return bytes: %ld\n", bytes, io_size);
				}
			}
			assert(bytes==io_size);
			
			//// Write new_ads_buffer to storage ////
			long idx = 0;
			for(unsigned cur_vid = beg_dst_vid; cur_vid < end_dst_vid; ++cur_vid){ // Convert ads_buffer into new_ads_buffer
				unsigned vid_off = cur_vid - beg_dst_vid;
				// Move active ADS-elements from ads_buffer to new_ads_buffer
				long beg_bias = vid_off * klogV + all_ads_cnt[dst_pid][vid_off];
				long end_bias = beg_bias + nxt_active_ads_idx[dst_pid][vid_off+1];
				for(long i = beg_bias; i < end_bias; ++i)
					new_ads_buffer[idx++] = ads_buffer[i];
			}
			
			for(unsigned vid_off = 0; vid_off < part_num_nodes; ++vid_off){ // Create nxt_active_ads_idx
				all_ads_cnt[dst_pid][vid_off] += nxt_active_ads_idx[dst_pid][vid_off+1];
			}
			
			for(unsigned vid_off = 1; vid_off <= part_num_nodes; ++vid_off){ // Create nxt_active_ads_idx
				nxt_active_ads_idx[dst_pid][vid_off] += nxt_active_ads_idx[dst_pid][vid_off-1];
			}
			assert(idx == nxt_active_ads_idx[dst_pid][end_dst_vid-beg_dst_vid]);
			
			//io_size = ((nxt_active_ads_idx[dst_pid][end_dst_vid-beg_dst_vid] * sizeof(ads_entry)) / 512 + 1) * 512;
			io_size = up_aligned((nxt_active_ads_idx[dst_pid][end_dst_vid-beg_dst_vid] * sizeof(ads_entry)), (long unsigned int)512);
			bytes = my_fwrite(nxt_fd_active_ads[dst_pid], new_ads_buffer, io_size, 0);
			if (debugmode1){
				if (bytes != io_size){
					printf("Unexpected IO error in writing new_ads_buffer into nxt_fd_active_ads:\n");
					printf("Return bytes: %ld, expected return bytes: %ld\n", bytes, io_size);
				}
			}
			assert(bytes == io_size);
		}
		// Swap cur_active and nxt_active
		swap(cur_active_ads_idx, nxt_active_ads_idx);
		swap(cur_fd_active_ads, nxt_fd_active_ads);
		
		active_src_vid.clear();
		for(int pid = 0; pid < num_part; ++pid){
			unsigned beg_vid = pid * part_num_nodes;
			unsigned end_vid = (pid+1) * part_num_nodes;
			end_vid = (num_nodes < end_vid)? num_nodes: end_vid;
			
			for(unsigned cur_vid = beg_vid; cur_vid < end_vid; ++cur_vid){
				unsigned vid_off = cur_vid - beg_vid;
				if (cur_active_ads_idx[pid][vid_off] < cur_active_ads_idx[pid][vid_off+1])
					active_src_vid.set_bit(cur_vid);
			}
		}
		
		for(int pid = 0; pid < num_part; ++pid){
			unsigned beg_vid = pid * part_num_nodes;
			unsigned end_vid = (pid+1) * part_num_nodes;
			end_vid = (num_nodes < end_vid)? num_nodes: end_vid;
			
			new_cnt += cur_active_ads_idx[pid][end_vid-beg_vid];
			
			for(unsigned vid_off = 0; vid_off <= part_num_nodes; ++vid_off){
				nxt_active_ads_idx[pid][vid_off] = 0;
			}
		}
		
		double iter_end_time = wtime();
		
		printf("%d: %lld %lld, takes time: %.2f\n", ++cur_iter, new_cnt, total_cnt, iter_end_time-iter_beg_time);
		fflush(stdout);
		total_cnt += new_cnt;
	}
	double end_time = wtime();
	cout << "Total Time = " << end_time - beg_time << ", " << total_cnt << endl;
	//cout << total_phase_one_time << " + " << total_phase_two_time << " = " << total_phase_one_time+total_phase_two_time << endl;


	// TODO: close fds
	close(fd_edge);
	for(int pid = 0; pid < num_part; ++pid){
		close(fd_ads[pid]);
		close(cur_fd_active_ads[pid]);
		close(nxt_fd_active_ads[pid]);
	}

	// TODO: free memory allocation
	free(new_ads_pool);
	free(ads_buffer_pool);
	free(edge_buffer_pool);
	delete [] edge_buffer;
	
	delete [] ads_off;
	delete [] new_ads_off;
	
	delete [] fd_ads;

	for(int pid = 0; pid < num_part; ++pid){
		delete [] all_ads_cnt[pid];
		delete [] cur_active_ads_idx[pid];
		delete [] nxt_active_ads_idx[pid];
	}
	delete [] all_ads_cnt;
	delete [] cur_active_ads_idx;
	delete [] nxt_active_ads_idx;

	delete [] ranks;
	delete [] threads;
	return 0;
}