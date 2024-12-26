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

#define debugmode1 (1) // io debugging
#define debugmode2 (1) // ads debugging

using namespace std;

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

int cur_iter = 0;

long part_ads_sz;
long io_part_ads_sz;

char* new_ads_pool;
char* ads_buffer_pool;
char* upd_buffer_pool;
char* edge_buffer_pool;
long buffer_size = 256 * 1024;
long batch_size = 256 * 1024;
long PAGESIZE = 4096;

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

long* req_pg_id;

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
				long ret_bytes = pwrite(fd, ads_buffer+bytes, one_gb, beg_off);
				assert(ret_bytes==one_gb);
				bytes += ret_bytes;
				beg_off += ret_bytes;
			}
			else{
				long new_length = length - bytes;
				assert(new_length < one_gb);
				long ret_bytes = pwrite(fd, ads_buffer+bytes, new_length, beg_off);
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
				long ret_bytes = pread(fd, ads_buffer+bytes, one_gb, beg_off);
				assert(ret_bytes==one_gb);
				bytes += ret_bytes;
				beg_off += ret_bytes;
			}
			else{
				long new_length = length - bytes;
				assert(new_length < one_gb);
				long ret_bytes = pread(fd, ads_buffer+bytes, new_length, beg_off);
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

inline std::tuple<unsigned, unsigned> get_vid_range(unsigned part_id, unsigned tid){
	unsigned beg_vid = 0, end_vid = 0;
	//
	// do something
	//
	return std::make_tuple(beg_vid, end_vid);
}

long fileSize(int fd) {
   struct stat s;
   if (fstat(fd, &s) == -1) {
      int saveErrno = errno;
      fprintf(stderr, "fstat(%d) returned errno=%d.", fd, saveErrno);
      return(-1);
   }
   return(s.st_size);
}

bool max_heap_insert(unsigned* max_heap, unsigned* length, unsigned inserted_val)
{
	if ((*length) == k){
		if (max_heap[0] <= inserted_val)
			return false;
		else{
			max_heap[0] = inserted_val;
			int cur_idx = 0;
			unsigned largest_rank_idx;
			while(true){
				int left_idx = 2 * cur_idx + 1;
				int right_idx = 2 * cur_idx + 2;
				
				if (left_idx < (*length) && max_heap[left_idx] > max_heap[cur_idx])
					largest_rank_idx = left_idx;
				else
					largest_rank_idx = cur_idx;
				
				if (right_idx < (*length) && max_heap[right_idx] > max_heap[largest_rank_idx])
					largest_rank_idx = right_idx;
				
				if (largest_rank_idx != cur_idx){
					unsigned tmp = max_heap[largest_rank_idx];
					max_heap[largest_rank_idx] = max_heap[cur_idx];
					max_heap[cur_idx] =  tmp;
					cur_idx = largest_rank_idx;
				}
				else return true;
			}
		}
	}
	else{
		assert(*length < k);
		max_heap[*length] = inserted_val;
		(*length) += 1;
		
		int cur_idx = (*length)-1;
		while(cur_idx > 0){
			int parent = (cur_idx - 1) / 2;
			if (max_heap[cur_idx] > max_heap[parent]){
				unsigned tmp = max_heap[cur_idx];
				max_heap[cur_idx] = max_heap[parent];
				max_heap[parent] = tmp;
				cur_idx = parent;
			}
			else return true;
		}
		return true;
	}
}

void compute_edge(int tid)
{
	while(true){
		unsigned grid_id, block_id;
		long beg_off, end_off; // [beg_off, end_off) indicates the targeted range of edges
		tie(grid_id, block_id, beg_off, end_off) = tasks.pop();
		
		if (grid_id == -1) break;
		
		long edge_beg_off = column_offset[grid_id] + block_id * buffer_size;
		long edge_end_off = edge_beg_off + buffer_size;
		if (edge_end_off > column_offset[grid_id+1])
			edge_end_off = column_offset[grid_id+1];
		
		if (edge_beg_off < edge_end_off){
			// Read an edge-block into local-edge-buffer
			long io_size = edge_end_off - edge_beg_off;
			assert((io_size%512) == 0);
			long bytes = pread(fd_edge, edge_buffer[tid], io_size, edge_beg_off);
			if (debugmode1){
				if (bytes!=io_size){
					printf("Unexpected IO error in reading edge-data:\n");
					printf("Return bytes: %ld, expected return bytes: %ld\n", bytes, io_size);
				}
			}
			assert(bytes==io_size);

			for(long off = 0; off < bytes; off+=8){
				unsigned src_vid = *(unsigned*)(edge_buffer[tid] + off);
				if (src_vid == -1) break;
				
				if (exist_src_vid.get_bit(src_vid) == 0)
					exist_src_vid.set_bit(src_vid);
			}
		}
	}
}


void max_heapify(ads_entry* heap, int root, int length)
{
	int leftChild = root*2 + 1;
	int rightChild = root*2 + 2;
	int maxNode = -1;

    if(leftChild < length && (ranks[heap[leftChild].root_vid] > ranks[heap[root].root_vid]))
        maxNode = leftChild;
    else
        maxNode = root;	

    if(rightChild < length && (ranks[heap[rightChild].root_vid] > ranks[heap[maxNode].root_vid]))
        maxNode = rightChild;
	
    if(maxNode != root){
		// swap, swap(data, root, maxNode);
		ads_entry tmp = heap[root];
		heap[root] = heap[maxNode];
		heap[maxNode] = tmp;
		
        max_heapify(heap, maxNode, length);
    }	
}

int nxt_ready_buffer = -1;
int ready_buffer = -1;
long new_ads_cnt = 0;
void compute_ads(int tid)
{
	long local_new_cnt = 0;
	/*unsigned* max_heap = new unsigned [k]{};
	unsigned* heap_length = new unsigned;
	(*heap_length) = 0;*/
	
	while(true){
		unsigned grid_id, block_id;
		long beg_off, end_off; // [beg_off, end_off) indicates the targeted range of edges
		tie(grid_id, block_id, beg_off, end_off) = tasks.pop();
		
		if (grid_id == -2){
			assert(false);
			//write_add(&my_lock1, 1); // inform master thread that this worker is done
			//while(my_lock2 != 1) { __asm volatile ("pause" ::: "memory"); } // wait for master thread to continue
			continue;
		}
		else if (grid_id == -1) break;

		int dst_grid_id = grid_id / num_part;
		int src_grid_id = grid_id - dst_grid_id * num_part;
		unsigned src_beg_vid = src_grid_id * part_num_nodes;
		unsigned dst_beg_vid = dst_grid_id * part_num_nodes;

		long edge_beg_off = column_offset[grid_id] + block_id * buffer_size;
		long edge_end_off = edge_beg_off + buffer_size;
		if (edge_end_off > column_offset[grid_id+1])
			edge_end_off = column_offset[grid_id+1];
		
		if (edge_beg_off == edge_end_off) continue;

		// Read an edge-block into local-edge-buffer
		long io_size = edge_end_off - edge_beg_off;
		assert((io_size%512) == 0);
		long bytes = pread(fd_edge, edge_buffer[tid], io_size, edge_beg_off);
		if (debugmode1){
			if (bytes!=io_size){
				printf("Unexpected IO error in reading edge-data:\n");
				printf("Return bytes: %ld, expected return bytes: %ld\n", bytes, io_size);
			}
		}
		assert(bytes==io_size);

		// Create index data for edge-block
		for(unsigned off_vid = 0; off_vid <= part_num_nodes; ++off_vid)
			block_idx[tid][off_vid] = 0;
		for(long off = 0; off < bytes; off+=8){
			unsigned src = *(unsigned*)(edge_buffer[tid] + off);
			if (src == -1) break;
			++block_idx[tid][src-src_beg_vid+1];
		}
		for(unsigned off_vid = 1; off_vid <= part_num_nodes; ++off_vid)
			block_idx[tid][off_vid] += block_idx[tid][off_vid-1];
		
		// IMPORTANT COMPONENT: Perform ADS Computation
		for(unsigned src_off_vid = 0; src_off_vid < part_num_nodes; ++src_off_vid){
			unsigned src_vid = src_off_vid + src_beg_vid;
			//if (exist_src_vid.get_bit(src_vid) == false) continue;
			//if (block_idx[tid][src_off_vid] == block_idx[tid][src_off_vid+1]) continue; 
			//if (new_ads_off[src_vid].beg_off == new_ads_off[src_vid].end_off) continue;

			long beg_new_ads_idx = cur_active_ads_idx[src_grid_id][src_off_vid];
			long end_new_ads_idx = cur_active_ads_idx[src_grid_id][src_off_vid+1];
			while(beg_new_ads_idx < end_new_ads_idx){ // iterate each entry
				ads_entry ele;
				if (ready_buffer == 1)
					ele = new_ads_buffer_1[beg_new_ads_idx];
				else
					ele = new_ads_buffer_2[beg_new_ads_idx];
				
				long beg_edge_off = 8 * block_idx[tid][src_off_vid];
				long end_edge_off = 8 * block_idx[tid][src_off_vid+1];
				while(beg_edge_off < end_edge_off){ // iterate each edge
					unsigned src = *(unsigned*)(edge_buffer[tid] + beg_edge_off);
					unsigned dst = *(unsigned*)(edge_buffer[tid] + beg_edge_off + 4);
					unsigned weight = 1;
					assert(src == src_vid);
					assert(src < num_nodes && dst < num_nodes);
					
					unsigned new_dist = ele.dist+weight;
					unsigned dst_off_vid = dst - dst_beg_vid;
					
					//if (new_dist == 31)
					//	cout << ele.root_vid << endl;
					
					long* cur_iter_ads_cnt = &nxt_active_ads_idx[dst_grid_id][dst_off_vid+1];
					long accu_cnt = all_ads_cnt[dst_grid_id][dst_off_vid] + nxt_active_ads_idx[dst_grid_id][dst_off_vid+1];
					assert(accu_cnt <= klogV);

					bool already_exist = false;
					ads_entry* heap = &ads_buffer[dst_off_vid * klogV];
					if (accu_cnt < k){
						
						for(long i = 0; i < accu_cnt; ++i){
							if (heap[i].root_vid == ele.root_vid){
								already_exist = true;
								i = accu_cnt;
							}
						}
						
						if (already_exist == false){
							//int record_rank;
							//if (accu_cnt == 0)
							//	record_rank = -1;
							//else
							//	record_rank = ranks[heap[0].root_vid];
							
							heap[accu_cnt] = make_ads_entry(ele.root_vid, new_dist);
							
							//cout << "Before: " << record_rank << " " << ranks[heap[accu_cnt].root_vid] << endl;
							
							heap[klogV-(*cur_iter_ads_cnt)-1] = heap[accu_cnt]; // record nxt active

							//if (accu_cnt == k){
							//	max_heapify(heap, 0, k);
							//}

							int cur_idx = accu_cnt;
							while(true){ // shift up
								int parent_idx = (cur_idx-1) / 2;
								if (cur_idx == 0 && parent_idx == 0){
									break;
								}
								else if (ranks[heap[parent_idx].root_vid] < ranks[heap[cur_idx].root_vid]){
								//else if (heap[parent_idx].root_vid < heap[cur_idx].root_vid) {
									// swap
									ads_entry tmp = heap[parent_idx];
									heap[parent_idx] = heap[cur_idx];
									heap[cur_idx] = tmp;
									
									cur_idx = parent_idx;
								}
								else break;
							}

							// ++nxt_active_ads_idx[dst_grid_id][dst_off_vid+1];
							++(*cur_iter_ads_cnt);
							++local_new_cnt;
							++accu_cnt;
							
							/*for(int i = 0; i < accu_cnt; ++i){
								int left_idx = 2 * i + 1;
								int right_idx = 2 * i + 2;
								if (left_idx < accu_cnt){
									bool check = (ranks[heap[i].root_vid] > ranks[heap[left_idx].root_vid]);
									if (check == false){
										cout << accu_cnt << endl;
										for(int i = 0; i < accu_cnt; ++i)
											cout << ranks[heap[i].root_vid] << " ";
										fflush(stdout);
									}
									assert(ranks[heap[i].root_vid] > ranks[heap[left_idx].root_vid]);
								}
								if (right_idx < accu_cnt){
									bool check = (ranks[heap[i].root_vid] > ranks[heap[right_idx].root_vid]);
									if (check == false){
										cout << accu_cnt << endl;
										for(int i = 0; i < accu_cnt; ++i)
											cout << ranks[heap[i].root_vid] << " ";
										fflush(stdout);
									}
									assert(ranks[heap[i].root_vid] > ranks[heap[right_idx].root_vid]);
								}
							}*/
							//cout << "After: " << record_rank << " " << ranks[heap[0].root_vid] << endl;
							
							assert(accu_cnt + (*cur_iter_ads_cnt) < klogV);
							//assert(record_rank <= (int)ranks[heap[0].root_vid]);
						}
					}
					else{
						if (ranks[heap[0].root_vid] > ranks[ele.root_vid]){
						//if (heap[0].root_vid > ele.root_vid){

							for(long i = 0; i < accu_cnt; ++i){
								if (heap[i].root_vid == ele.root_vid){
									already_exist = true;
									i = accu_cnt;
								}
							}
							
							if (already_exist == false){
								//int record_rank = ranks[heap[0].root_vid];
								//int new_rank = ranks[ele.root_vid];
								
								/*for(int i = 0; i < k; ++i){
									int left_idx = 2 * i + 1;
									int right_idx = 2 * i + 2;
									if (left_idx < k){
										bool check = (ranks[heap[i].root_vid] > ranks[heap[left_idx].root_vid]);
										if (check == false){
											cout << "before: " << k << ", " << new_rank << endl;
											for(int i = 0; i < k; ++i)
												cout << ranks[heap[i].root_vid] << " ";
											fflush(stdout);
										}
										assert(ranks[heap[i].root_vid] > ranks[heap[left_idx].root_vid]);
									}
									if (right_idx < k){
										bool check = (ranks[heap[i].root_vid] > ranks[heap[right_idx].root_vid]);
										if (check == false){
											cout << "before: " << k << ", " << new_rank << endl;
											for(int i = 0; i < k; ++i)
												cout << ranks[heap[i].root_vid] << " ";
											fflush(stdout);
										}
										assert(ranks[heap[i].root_vid] > ranks[heap[right_idx].root_vid]);
									}
								}*/
								
								heap[accu_cnt] = heap[0];
								heap[0] = make_ads_entry(ele.root_vid, new_dist);
								
								//cout << "Before: " << record_rank << " " << ranks[heap[0].root_vid] << endl;
								
								heap[klogV-(*cur_iter_ads_cnt)-1] = heap[0]; // record nxt active
								assert(klogV-(*cur_iter_ads_cnt)-1 > accu_cnt);
								
								int cur_idx = 0;
								while(true){ // shift down
									int max_idx = -1;
									int left_idx = cur_idx * 2 + 1;
									int right_idx = cur_idx * 2 + 2;
									if (left_idx < k){
										if (ranks[heap[left_idx].root_vid] > ranks[heap[cur_idx].root_vid]){
										//if (heap[left_idx].root_vid > heap[cur_idx].root_vid){
											max_idx = left_idx;
										}
									}
									
									if (max_idx == -1)
										max_idx = cur_idx;
									
									if (right_idx < k){
										if (ranks[heap[right_idx].root_vid] > ranks[heap[max_idx].root_vid]){
										//if (heap[right_idx].root_vid > heap[max_idx].root_vid){
											max_idx = right_idx;
										}
									}
									
									assert(max_idx != -1);
									if (max_idx == cur_idx)
										break;
									
									// swap
									ads_entry tmp = heap[max_idx];
									heap[max_idx] = heap[cur_idx];
									heap[cur_idx] = tmp;
									
									cur_idx = max_idx;
								}

								// ++nxt_active_ads_idx[dst_grid_id][dst_off_vid+1];
								++(*cur_iter_ads_cnt);
								++local_new_cnt;
								
								/*for(int i = 0; i < k; ++i){
									int left_idx = 2 * i + 1;
									int right_idx = 2 * i + 2;
									if (left_idx < k){
										bool check = (ranks[heap[i].root_vid] > ranks[heap[left_idx].root_vid]);
										if (check == false){
											cout << "after: " << k << ", " << new_rank << endl;
											for(int i = 0; i < k; ++i)
												cout << ranks[heap[i].root_vid] << " ";
											fflush(stdout);
										}
										assert(ranks[heap[i].root_vid] > ranks[heap[left_idx].root_vid]);
									}
									if (right_idx < k){
										bool check = (ranks[heap[i].root_vid] > ranks[heap[right_idx].root_vid]);
										if (check == false){
											cout << "after: " << k << ", " << new_rank << endl;
											for(int i = 0; i < k; ++i)
												cout << ranks[heap[i].root_vid] << " ";
											fflush(stdout);
										}
										assert(ranks[heap[i].root_vid] > ranks[heap[right_idx].root_vid]);
									}
								}*/
								
								//cout << "After: " << record_rank << " " << ranks[heap[0].root_vid] << endl;
								
								assert(accu_cnt + (*cur_iter_ads_cnt) < klogV);
								//assert(record_rank > (int)ranks[heap[0].root_vid]);
							}
						}
					}
					beg_edge_off += 8;

				}
				
				++beg_new_ads_idx;
			}
			
		}
	}
}

void create_edge_layout(string input_path, string output_dir)
{
	// Divide the graph into P partitions by splitting the V into P disjoint-and-equal set.
	assert((num_nodes%num_part)==0);
	unsigned part_num_nodes = num_nodes / num_part;
	
	long e_buffer_size = 4 * 1024 * 1024;
	char* e_buffer = (char*)memalign(4096, e_buffer_size);
	assert(e_buffer != NULL);
	int fin = open(input_path.c_str(), O_RDONLY);
	assert(fin!=-1);
	
	unsigned* outdeg = new unsigned [num_nodes]{};
	//unsigned* indeg = new unsigned [num_nodes]{};
	long* edges_grid = new long [num_part * num_part]{};
	long total_edges = 0;
	
	long total_bytes = file_size(input_path);
	assert((total_bytes%8)==0);
	num_edges = total_bytes/8;
	
	ranks = new unsigned [num_nodes]{};
	for(unsigned vid = 0; vid < num_nodes; ++vid)
		ranks[vid] = vid;
	//srand(0); // -> srand(time(0));
	//random_device rd;
	//mt19937 g(rd());
	//shuffle(ranks, ranks+num_nodes, g); // rank value tie-breaking
	
	long off = 0;
	while(off < total_bytes){
		long io_size = e_buffer_size;
		if (off + io_size > total_bytes){
			io_size = total_bytes - off;
		}
		long bytes = pread(fin, e_buffer, io_size, off);
		assert(bytes==io_size);
		off += io_size;
		
		for(long off = 0; off < io_size; off+=8){
			unsigned src = *(unsigned*)(e_buffer + off);
			unsigned dst = *(unsigned*)(e_buffer + off + 4);
			
			// rank-reordering
			src = ranks[src];
			dst = ranks[dst];
			
			++outdeg[src];
			//++indeg[dst];
			
			int src_part_id = src / part_num_nodes;
			int dst_part_id = dst / part_num_nodes;
			++edges_grid[dst_part_id * num_part + src_part_id];
			++total_edges;
		}
	}
	assert(num_edges == total_edges);
	
	long* grid_off = new long [num_part * num_part + 1]{};
	for(int grid_id = 0; grid_id < num_part * num_part; ++grid_id){
		grid_off[grid_id+1] += grid_off[grid_id] + edges_grid[grid_id];
	}
	
	long* ori_out_idx = new long [num_nodes+1]{};
	for(unsigned vid = 1; vid <= num_nodes; ++vid){
		ori_out_idx[vid] = (ori_out_idx[vid-1] + outdeg[vid-1]);
	}
	unsigned* ori_edges = new unsigned [2*total_edges]{};
	off = 0;
	while(off < total_bytes){
		long io_size = e_buffer_size;
		if (off + io_size > total_bytes){
			io_size = total_bytes - off;
		}
		long bytes = pread(fin, e_buffer, io_size, off);
		assert(bytes==io_size);
		off += io_size;
		
		for(long off = 0; off < io_size; off+=8){
			unsigned src = *(unsigned*)(e_buffer + off);
			unsigned dst = *(unsigned*)(e_buffer + off + 4);
			
			// rank-reordering
			src = ranks[src];
			dst = ranks[dst];
			
			long idx = 2 * ori_out_idx[src];
			assert(ori_edges[idx] == 0);
			ori_edges[idx] = src;
			assert(ori_edges[idx+1] == 0);
			ori_edges[idx+1] = dst;
			++ori_out_idx[src];
		}
	}
	delete [] ori_out_idx;
	
	
	long* cur_edges_grid = new long [num_part * num_part]{};
	unsigned* edges = new unsigned [2*total_edges]{};
	for (long e_idx = 0; e_idx < total_edges; ++e_idx){
		long idx = 2 * e_idx;
		unsigned src = ori_edges[idx];
		unsigned dst = ori_edges[idx+1];
		
		int src_part_id = src / part_num_nodes;
		int dst_part_id = dst / part_num_nodes;
		int cur_grid_id = dst_part_id * num_part + src_part_id;
		
		long grid_edge_idx = 2 * (grid_off[cur_grid_id] + cur_edges_grid[cur_grid_id]);
		assert(edges[grid_edge_idx]==0);
		edges[grid_edge_idx] = src;
		assert(edges[grid_edge_idx+1]==0);
		edges[grid_edge_idx+1] = dst;
		
		++cur_edges_grid[cur_grid_id];
	}
	
	/*off = 0;
	while(off < total_bytes){
		long io_size = e_buffer_size;
		if (off + io_size > total_bytes){
			io_size = total_bytes - off;
		}
		long bytes = pread(fin, e_buffer, io_size, off);
		assert(bytes==io_size);
		off += io_size;
		
		for(long off = 0; off < io_size; off+=8){
			unsigned src = *(unsigned*)(e_buffer + off);
			unsigned dst = *(unsigned*)(e_buffer + off + 4);
			
			// rank-reordering
			src = ranks[src];
			dst = ranks[dst];
			
			int src_part_id = src / part_num_nodes;
			int dst_part_id = dst / part_num_nodes;
			int cur_grid_id = dst_part_id * num_part + src_part_id;
			
			long edge_off = 2 * (grid_off[cur_grid_id] + cur_edges_grid[cur_grid_id]);
			assert(edges[edge_off]==0);
			edges[edge_off] = src;
			assert(edges[edge_off+1]==0);
			edges[edge_off+1] = dst;
			
			++cur_edges_grid[cur_grid_id];
		}
	}*/
	close(fin);

	int fout_column = open((output_dir+"/column").c_str(), O_WRONLY|O_APPEND|O_CREAT|O_TRUNC);
	int fout_column_offset = open((output_dir+"/column_offset").c_str(), O_WRONLY|O_APPEND|O_CREAT|O_TRUNC);
	//int fout_blk_vid_range = open((output_dir+"/blk_vid_range").c_str(), O_WRONLY|O_APPEND|O_CREAT|O_TRUNC, 0644);
	
	/*long** part_indeg = new long* [num_part * num_part]{};
	for(int grid_id = 0; grid_id < num_part * num_part; ++grid_id){
		part_indeg[grid_id] = new long [part_num_nodes]{};
		
		int dst_part_id = grid_id / num_part;
		int src_part_id = grid_id - dst_part_id * num_part;
		unsigned src_beg_vid = src_part_id * part_num_nodes;
		unsigned src_end_vid = src_beg_vid + part_num_nodes;
		unsigned dst_beg_vid = dst_part_id * part_num_nodes;
		unsigned dst_end_vid = dst_beg_vid + part_num_nodes;
		
		long beg_off = 2 * grid_off[grid_id];
		long end_off = 2 * grid_off[grid_id+1];
		while(beg_off < end_off){
			unsigned src = edges[beg_off];
			unsigned dst = edges[beg_off+1];
			assert(src_beg_vid <= src && src < src_end_vid);
			assert(dst_beg_vid <= dst && dst < dst_end_vid);
			
			++part_indeg[grid_id][dst-dst_beg_vid];
			
			beg_off += 2;
		}
	}*/
	
	long my_total_size = 0;
	long write_edges = 0;
	long offset = 0;
	for(int col_part_id = 0; col_part_id < num_part; ++col_part_id){
		unsigned beg_dst_vid = col_part_id * part_num_nodes;
		
		// TODO: write the edge of each block into files
		long col_edges = grid_off[(col_part_id+1)*num_part+1] - grid_off[col_part_id*num_part];

		for(int row_part_id = 0; row_part_id < num_part; ++row_part_id){
			int grid_id = col_part_id * num_part + row_part_id;
			
			unsigned* indeg = new unsigned [part_num_nodes]{};
			int* vid_to_bid = new int [part_num_nodes]{};
			long beg_off = 2 * grid_off[grid_id];
			long end_off = 2 * grid_off[grid_id+1];
			
			long cur_off = beg_off;
			while(cur_off < end_off){
				unsigned src = edges[cur_off];
				unsigned dst = edges[cur_off+1];
				assert(dst>=beg_dst_vid);
				unsigned dst_off = dst - beg_dst_vid;

				++indeg[dst_off];
				cur_off += 2;
			}
			
			long accu_size = 0;
			int cur_bid = 0;
			for(unsigned dst_vid_off = 0; dst_vid_off < part_num_nodes; ++dst_vid_off){
				long esz = 8*indeg[dst_vid_off];
				if (accu_size + esz > buffer_size){
					++cur_bid;
					accu_size = esz;
					vid_to_bid[dst_vid_off] = cur_bid;
				}
				else{
					accu_size += esz;
					vid_to_bid[dst_vid_off] = cur_bid;
				}
			}
			//assert(accu_size > 0);
			
			unsigned num_blocks = cur_bid+1;
			long aligned_accu_size = accu_size;
			if ((aligned_accu_size%4096) != 0)
				aligned_accu_size = (aligned_accu_size/4096+1)*4096;
			long real_io_size = (num_blocks-1) * buffer_size + aligned_accu_size;
			my_total_size += real_io_size;
			
			if (accu_size == 0)
				assert(real_io_size == accu_size);
			
			if (accu_size > 0){
				assert((real_io_size%4096) == 0);
				char* runtime_buffer = (char*)memalign(4096, real_io_size); // can hold all the edges in a grid, write to column
				long* blk_off = new long [num_blocks]{};
				for(unsigned blk_id = 0; blk_id < num_blocks; ++blk_id)
					blk_off[blk_id] = blk_id * buffer_size;
				
				cur_off = beg_off;
				while(cur_off < end_off){
					unsigned src = edges[cur_off];
					unsigned dst = edges[cur_off+1];
					unsigned dst_off = dst - beg_dst_vid;

					int this_bid = vid_to_bid[dst_off];
					
					long off = blk_off[this_bid];
					*(unsigned*)(runtime_buffer + off) = src;
					*(unsigned*)(runtime_buffer + off + 4) = dst;
					blk_off[this_bid] += 8;
					
					cur_off += 2;
					
					++write_edges;
				}
				
				for(unsigned blk_id = 0; blk_id < num_blocks; ++blk_id){
					long off = blk_off[blk_id];
					if (blk_id != num_blocks-1 && off < (blk_id+1)*buffer_size){
						*(unsigned*)(runtime_buffer + off) = -1;
						*(unsigned*)(runtime_buffer + off + 4) = -1;
						blk_off[blk_id] += 8;
						assert(blk_off[blk_id] <= (blk_id+1)*buffer_size);
					}
					else if (blk_id == num_blocks-1 && off < real_io_size){
						*(unsigned*)(runtime_buffer + off) = -1;
						*(unsigned*)(runtime_buffer + off + 4) = -1;
						blk_off[blk_id] += 8;
						assert(blk_off[blk_id] <= real_io_size);
					}
				}

				//long bytes = write(fout_column, runtime_buffer, real_io_size);
				long bytes = my_fwrite(fout_column, runtime_buffer, real_io_size, offset);
				assert(bytes == real_io_size);
				
				delete [] blk_off;
				free(runtime_buffer);
			}
			long bytes = write(fout_column_offset, &offset, sizeof(offset));
			assert(bytes == sizeof(offset));
			offset += real_io_size;
			
			
			delete [] indeg;
			delete [] vid_to_bid;
			
			cout << grid_id << ": " << num_blocks << " blocks <-> " << 8 * (grid_off[grid_id+1] - grid_off[grid_id]) << " real edge bytes" << endl;
		}
	}
	write(fout_column_offset, &offset, sizeof(offset));

	if (write_edges != num_edges){
		cout << write_edges << " " << num_edges << endl;
		fflush(stdout);
		assert(write_edges == num_edges);
	}

	int edge_type = 0;
	FILE* fmeta = fopen((output_dir+"/meta").c_str(), "w");
	fprintf(fmeta, "%d %d %ld %d", edge_type, num_nodes, num_edges, num_part);
	
	cout << my_total_size << endl;
	fflush(stdout);
	
	close(fout_column_offset);
	close(fout_column);
	fclose(fmeta);
	
	/*for(int grid_id = 0; grid_id < num_part * num_part; ++grid_id)
		delete [] part_indeg[grid_id];
	delete [] part_indeg;*/
	
	delete [] grid_off;
	delete [] cur_edges_grid;
	delete [] edges_grid;
	//delete [] out_idx;
	delete [] edges;
	delete [] ori_edges;
	delete [] outdeg;
	delete [] ranks;
	//delete [] indeg;
	free(e_buffer);
	
}

int use_rank_reordering;
void init(string path)
{
	// read meta
	FILE * fin_meta = fopen((path+"meta").c_str(), "r");
	fscanf(fin_meta, "%d %ld %ld %d", &edge_type, &num_nodes, &num_edges, &num_part);
	fclose(fin_meta);
	cout << path << ", K = " << k << ", NUM_THD = " << NUM_THD << endl;
	cout << edge_type << " " << num_nodes << " " << num_edges << " " << num_part << endl;
	
	assert((num_nodes%num_part) == 0);
	part_num_nodes = num_nodes / num_part;
	num_grids = num_part * num_part;
	
	column_offset = new long [num_grids+1];
	int fin_column_offset = open((path+"column_offset").c_str(), O_RDONLY);
	long bytes = read(fin_column_offset, column_offset, sizeof(long)*(num_grids+1));
	assert(bytes==sizeof(long)*(num_grids+1));
	close(fin_column_offset);

	fd_edge = open((path+"column").c_str(), O_DIRECT | O_RDONLY); // edge_buffer
	if (fd_edge == -1){
		cout << "edge file open error!" << endl;
		assert(0==1);
	}
	
	cur_fd_active_ads = new int [num_part]{};
	nxt_fd_active_ads = new int [num_part]{};
	cur_active_ads_idx = new long* [num_part]{};
	nxt_active_ads_idx = new long* [num_part]{};
	for (int pid = 0; pid < num_part; ++pid){
		string pid_str = to_string(pid);
		cur_fd_active_ads[pid] = open((path+"cur_active_ads.p"+pid_str).c_str(), O_RDWR | O_CREAT | O_TRUNC | O_DIRECT);
		nxt_fd_active_ads[pid] = open((path+"nxt_active_ads.p"+pid_str).c_str(), O_RDWR | O_CREAT | O_TRUNC | O_DIRECT);
		if (cur_fd_active_ads[pid] == -1 || nxt_fd_active_ads[pid] == -1){
			cout << "fd_active_ads open error!" << endl;
			assert(0 == 1);
		}
		
		cur_active_ads_idx[pid] = new long [part_num_nodes+1]{};
		nxt_active_ads_idx[pid] = new long [part_num_nodes+1]{};
	}

	fd_ads = new int [num_part];
	all_ads_cnt = new long* [num_part]{};
	for (int pid = 0; pid < num_part; ++pid){
		string pid_str = to_string(pid);
		fd_ads[pid] = open((path+"all_ads.p"+pid_str).c_str(), O_RDWR | O_CREAT | O_TRUNC | O_DIRECT); // ads_buffer
		if (fd_ads[pid] == -1){
			cout << "ads file open error!" << endl;
			assert(0 == 1);
		}
		
		all_ads_cnt[pid] = new long [part_num_nodes]{};
	}

	klogV = k * (long)(log2(num_nodes)) * 2;
	part_ads_sz = part_num_nodes * klogV * sizeof(ads_entry) * 2;
	//long io_part_ads_sz = (part_ads_sz / 512 + 1) * 512;
	io_part_ads_sz = up_aligned(part_ads_sz, (long)4096);

	cout << klogV << endl;

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
	
	//sampled_cannot_ads = new ads_entry [num_nodes]{};
	//for(unsigned vid = 0; vid < num_nodes; ++vid){
	//	sampled_cannot_ads[vid].root_vid = -1;
	//	sampled_cannot_ads[vid].dist = -1;
	//}
	
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
		
		bytes = my_fwrite(cur_fd_active_ads[part_id], new_ads_buffer_1, io_part_ads_sz/2, 0);
		assert(bytes==io_part_ads_sz/2);
		
		bytes = my_fwrite(nxt_fd_active_ads[part_id], new_ads_buffer_1, io_part_ads_sz/2, 0);
		assert(bytes==io_part_ads_sz/2);
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
	for(unsigned vid = 0; vid < num_nodes; ++vid)
		ranks[vid] = vid;
	if (!use_rank_reordering){
		srand(0); // -> srand(time(0));
		random_device rd;
		mt19937 g(rd());
		shuffle(ranks, ranks+num_nodes, g); // rank value tie-breaking
	}
}

// Graph: social-LiveJournal
// g++ test_oasis_create_ads.cpp -lpthread -laio -O3 -o oasis_ads_creation.out
// g++ no_separation_oasis_create_ads.cpp -lpthread -laio -O3 -o oasis_ads_creation_no_separation.out

// preprocessing:
// sudo ./oasis_ads_creation_no_pipeline.out 2 /data/nvme_raid/soc-LJ.rank.8p/ 4 16
// sudo ./oasis_ads_creation.out 2 /data/nvme_raid/soc-LJ.rank.8p/ 4 16

// 
// ./a.out 1 /home/yangty/graph_data/binary_graph/twitter-mpi.origin.bin /home/yangty/ADS/Oasis/data/twitter.origin.8p/ 41652544 8 
// ./a.out 1 /home/yangty/graph_data/binary_graph/com-orkut.origin.bin /home/yangty/ADS/Oasis/data/com-orkut.origin.8p/ 3072632 8 

// execution:
// sudo cp -r /home/yangty/ADS/Oasis/data/soc-LJ.origin.8p/ /data/nvme_raid/
// sudo nohup ./oasis_ads_creation.out 2 /data/nvme_raid/soc-LJ.origin.8p/ 16 16 1>nohup.oasis.soc-LJ.8p.k16.origin.create_ads.out 2>&1 &
// sudo rm -r /data/nvme_raid/soc-LJ.origin.8p/

// sudo ./a.out 2 /data/nvme_raid/soc-LJ.origin.8p/ 16 16
// nohup.oasis.soc-LJ.8p.k64.origin.create_ads.out

//// OLD:
// sudo ./a.out 1 /data/nvme_raid/yangty/ADS/soc_column /data/nvme_raid/yangty/ADS/soc-LiveJournal.2p.grid_block/ 4847616 2   (136, 14, 14, 14)
// sudo ./a.out 1 /data/nvme_raid/yangty/ADS/soc_column /data/nvme_raid/yangty/ADS/soc-LiveJournal.16p.grid_block/ 4847616 16
// sudo ./a.out 1 /data/nvme_raid/yangty/ADS/soc-LJ.bin /data/nvme_raid/yangty/ADS/soc-LiveJournal.32p.ads/ 4847616 32
// sudo ./a.out 2 /data/nvme_raid/yangty/ADS/soc-LiveJournal.2p.grid_block/ 1 16

// Graph: Twitter
// sudo ./a.out 1 /data/nvme_raid/yangty/ADS/tw_column /data/nvme_raid/yangty/ADS/twitter.64p.grid_block/ 41652544 64
// sudo ./a.out 1 /data/nvme_raid/yangty/ADS/tw_column /data/nvme_raid/yangty/ADS/twitter.32p.grid_block/ 41652544 32
// sudo ./a.out 1 /data/nvme_raid/yangty/ADS/tw_column /data/nvme_raid/yangty/ADS/twitter.16p.grid_block/ 41652544 16
// sudo ./a.out 2 /data/nvme_raid/yangty/ADS/twitter.16p.grid_block/ 4 8

// sudo ./a.out 1 /data/nvme_raid/yangty/ADS/twitter-mpi.origin.bin /data/nvme_raid/yangty/ADS/twitter.32p.ads/ 41652544 32
// sudo ./a.out 2 /data/nvme_raid/yangty/ADS/twitter.32p.ads/ 4 16 
// sudo nohup ./a.out 2 /data/nvme_raid/yangty/ADS/twitter.32p.ads/ 4 16 1>nohup.oasis.twitter.32p.k4.rank.create_ads.out 2>&1 &
// sudo nohup ./a.out 2 /data/nvme_raid/yangty/ADS/twitter.32p.ads/ 4 16 1>nohup.oasis-no-selec.twitter.32p.k4.rank.create_ads.out 2>&1 &

// sudo nohup ./a.out 2 /data/nvme_raid/yangty/ADS/soc-LiveJournal.32p.ads/ 8 16 1>nohup.oasis.soc-LJ.32p.k8.rank.create_ads.out 2>&1 &
// sudo nohup ./a.out 2 /data/nvme_raid/yangty/ADS/soc-LiveJournal.32p.ads/ 8 16 1>nohup.oasis-no-selec.soc-LJ.32p.k8.rank.create_ads.out 2>&1 &

void test_run_bfs()
{
	unsigned root = 822031;
	bool* visited = new bool [num_nodes]{};
	unsigned accu_active_cnt = 0;
	
	bool* active_vid =  new bool [num_nodes]{};
	unsigned active_cnt = 0;
	
	bool* nxt_active_vid = new bool [num_nodes]{};
	unsigned nxt_active_cnt = 0;
	
	visited[root] = true;
	active_vid[root] = true;
	active_cnt++;
	accu_active_cnt++;
	while(active_cnt > 0){
		
		for(int dst_pid = 0; dst_pid < num_part; ++dst_pid){
			for(int src_pid = 0; src_pid < num_part; ++src_pid){ // assign tasks
				int grid_id = dst_pid * num_part + src_pid;
				int num_blocks;
				long grid_size = (column_offset[grid_id+1] - column_offset[grid_id]);
				if ((grid_size%buffer_size) == 0) num_blocks = grid_size / buffer_size;
				else num_blocks = grid_size / buffer_size + 1;

				for(int block_id = 0; block_id < num_blocks; ++block_id){
					long edge_beg_off = column_offset[grid_id] + block_id * buffer_size;
					long edge_end_off = edge_beg_off + buffer_size;
					if (edge_end_off > column_offset[grid_id+1])
						edge_end_off = column_offset[grid_id+1];
					
					if (edge_beg_off < edge_end_off){
						// Read an edge-block into local-edge-buffer
						long io_size = edge_end_off - edge_beg_off;
						assert((io_size%512) == 0);
						long bytes = pread(fd_edge, edge_buffer[0], io_size, edge_beg_off);
						if (debugmode1){
							if (bytes!=io_size){
								printf("Unexpected IO error in reading edge-data:\n");
								printf("Return bytes: %ld, expected return bytes: %ld\n", bytes, io_size);
							}
						}
						assert(bytes==io_size);

						for(long off = 0; off < bytes; off+=8){
							unsigned src_vid = *(unsigned*)(edge_buffer[0] + off);
							unsigned dst_vid = *(unsigned*)(edge_buffer[0] + off + 4);
							if (src_vid == -1) break;

							if (active_vid[src_vid] == true && visited[dst_vid] == false){
								assert(visited[src_vid]==true);
								visited[dst_vid] = true;
								nxt_active_vid[dst_vid] = true;
								++nxt_active_cnt;
							}
						}
					}
				}
			}
		}
		
		active_cnt = nxt_active_cnt;
		nxt_active_cnt = 0;
		
		bool* tmp = active_vid;
		active_vid = nxt_active_vid;
		nxt_active_vid = tmp;
		
		for(unsigned vid = 0; vid < num_nodes; ++vid)
			nxt_active_vid[vid] = false;
		
		++cur_iter;
		cout << cur_iter << ": " << active_cnt << endl;
		accu_active_cnt += active_cnt;
		fflush(stdout);
	}
	
	unsigned total_visited = 0;
	for(unsigned vid = 0; vid < num_nodes; ++vid)
		if (visited[vid] == true)
			++total_visited;
	cout << accu_active_cnt << " " << total_visited << endl;
	fflush(stdout);
	
	delete [] visited;
	delete [] active_vid;
	delete [] nxt_active_vid;
}

int main(int argc, char ** argv)
{
	int model_selection = atol(argv[1]);
	
	if (model_selection == 1){ // do preprocessing
		string graph_path = argv[2];
		string output_path = argv[3];
		num_nodes = atol(argv[4]);
		num_part = atol(argv[5]);
		//int apply_random_order = atol(argv[6]);

		/*ranks = new unsigned [num_nodes]{};
		for(unsigned vid = 0; vid < num_nodes; ++vid)
			ranks[vid] = vid;
		if (apply_random_order == 1){ // Randomly assigning rank value 
			srand(0); // -> srand(time(0));
			random_device rd;
			mt19937 g(rd());
			shuffle(ranks, ranks+num_nodes, g); // rank value tie-breaking
		}
		else assert(apply_random_order==0);*/
		
		//assert(apply_random_order==1);
		create_edge_layout(graph_path, output_path);
		
		//delete [] ranks;
		return 0;
	}
	assert(model_selection == 2); // do ads_construction
	string path = argv[2];
	k = atol(argv[3]);
	NUM_THD = atol(argv[4]);
	use_rank_reordering = 0;
	
	init(path);
	
	//test_run_bfs();
	//return 0;
	
	// Begin ADS construction init
	cout << "Start ADS Construction (begin initialization)" << endl;
	
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

		// long io_size = up_aligned((idx * sizeof(ads_entry)), 512);
		long io_size = up_aligned(io_part_ads_sz/2, 512);
		long bytes = my_fwrite(cur_fd_active_ads[pid], new_ads_buffer, io_size, 0); // write active elements
		assert(bytes == io_size);
	}
	new_cnt = num_nodes;
	cur_iter = 0;
	threads = new thread [NUM_THD]{};
	cout << "End initialization" << endl;
	printf("%d: %lld %lld, takes time: %.2f (%.2f + %.2f)\n", cur_iter, new_cnt, total_cnt, wtime()-beg_time);
	fflush(stdout);
	// End ADS construction init
	
	unsigned* order_dst_pid = new unsigned [num_part];
	unsigned* order_src_pid = new unsigned [num_part];
	for(unsigned pid = 0; pid < num_part; ++pid){
		order_dst_pid[pid] = pid;
		order_src_pid[pid] = pid;
	}
	
	while(new_cnt > 0) {
		new_cnt = 0;
		
		double iter_beg_time = wtime();
		
		double read_io_time = 0;
		double write_io_time = 0;
		//random_device rd;
		//mt19937 g(rd());
		//shuffle(order_dst_pid, order_dst_pid+num_part, g);
		for(int dst_pid_idx = 0; dst_pid_idx < num_part; ++dst_pid_idx){
			unsigned dst_pid = order_dst_pid[dst_pid_idx];
			
			unsigned beg_dst_vid = dst_pid * part_num_nodes;
			unsigned end_dst_vid = (dst_pid+1) * part_num_nodes;
			end_dst_vid = (num_nodes < end_dst_vid)? num_nodes: end_dst_vid;
			
			//** Enabling Selective Access **//
			if (true){
			//if (false){
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
				
				double tmp_time = wtime();
				io_size = io_part_ads_sz;
				bytes = my_fread(fd_ads[dst_pid], ads_buffer, io_size, 0);
				if (debugmode1){
					if (bytes != io_size){
						printf("Unexpected IO error in reading fd_ads into ads_buffer:\n");
						printf("Return bytes: %ld, expected return bytes: %ld\n", bytes, io_size);
					}
				}
				assert(bytes == io_size);
				read_io_time += (wtime()-tmp_time);
				
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
				double tmp_time = wtime();
				long bytes = my_fread(fd_ads[dst_pid], ads_buffer, io_part_ads_sz, 0);
				if (debugmode1){
					if (bytes != io_part_ads_sz){
						printf("Unexpected IO error in reading fd_ads into ads_buffer:\n");
						printf("Return bytes: %ld, expected return bytes: %ld\n", bytes, io_part_ads_sz);
					}
				}
				assert(bytes == io_part_ads_sz);
				read_io_time += (wtime()-tmp_time);
				
				for(unsigned i = 0; i < active_src_vid.num_byte; ++i){
					// exist_src_vid.data[i] = 255;
					exist_src_vid.data[i] = active_src_vid.data[i];
				}
			}
			
			// Selectively load src_pid 0
			/*unsigned beg_src_vid = 0;
			unsigned end_src_vid = part_num_nodes;
			end_src_vid = (num_nodes < end_src_vid)? num_nodes: end_src_vid;
			unsigned num_pg = 0;
			for(unsigned cur_src_vid = beg_src_vid; cur_src_vid < end_src_vid; ++cur_src_vid){
				if (exist_src_vid.get_bit(cur_src_vid) == true){
					unsigned bias_vid =  cur_src_vid - beg_src_vid;
					long beg_off = cur_active_ads_idx[0][bias_vid];
					long end_off = cur_active_ads_idx[0][bias_vid+1];
					
					long beg_pg_id = beg_off / PAGESIZE;
					long end_pg_id = (end_off-1) / PAGESIZE;
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
			assert(num_issued == num_pg);*/
			
			// long io_size = up_aligned((cur_active_ads_idx[0][part_num_nodes] * sizeof(ads_entry)), 512);
			long io_size = up_aligned(io_part_ads_sz/2, 512);
			long bytes = my_fread(cur_fd_active_ads[0], new_ads_buffer_1, io_size, 0);
			assert(bytes == io_size);
			nxt_ready_buffer = 1;

			// Parallelly perform I/O and computation
			//random_device rd;
			//mt19937 g(rd());
			//shuffle(order_src_pid, order_src_pid+num_part, g);
			for(int src_pid_idx = 0; src_pid_idx < num_part; ++src_pid_idx){
				int src_pid = order_src_pid[src_pid_idx];
				
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
				//double my_beg_tm = wtime();
				

				
				int nxt_src_pid = src_pid+1;
				if (nxt_src_pid < num_part){
					//int nxt_src_pid = order_src_pid[src_pid_idx+1];
					
					/*unsigned beg_src_vid = nxt_src_pid * part_num_nodes;
					unsigned end_src_vid = (nxt_src_pid+1) * part_num_nodes;
					end_src_vid = (num_nodes < end_src_vid)? num_nodes: end_src_vid;
					unsigned num_pg = 0;
					for(unsigned cur_src_vid = beg_src_vid; cur_src_vid < end_src_vid; ++cur_src_vid){
						if (exist_src_vid.get_bit(cur_src_vid) == true){
							unsigned bias_vid =  cur_src_vid - beg_src_vid;
							long beg_off = cur_active_ads_idx[nxt_src_pid][bias_vid];
							long end_off = cur_active_ads_idx[nxt_src_pid][bias_vid+1];
							
							long beg_pg_id = beg_off / PAGESIZE;
							long end_pg_id = (end_off-1) / PAGESIZE;
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
					if (false && ready_buffer == 1){
						num_issued = aio_read(0, req_pg_id, num_pg, new_ads_buffer_2, io_part_ads_sz/2, nxt_src_pid);
						assert(num_issued == num_pg);
						nxt_ready_buffer = 2;
					}
					else if (true || ready_buffer == 2){
						num_issued = aio_read(0, req_pg_id, num_pg, new_ads_buffer_1, io_part_ads_sz/2, nxt_src_pid);
						assert(num_issued == num_pg);
						nxt_ready_buffer = 1;
					}
					else{
						bool wrong_ready_buffer_val = false;
						assert(wrong_ready_buffer_val);
					}
					assert(num_issued == num_pg);*/

					unsigned beg_src_vid = nxt_src_pid * part_num_nodes;
					unsigned end_src_vid = (nxt_src_pid+1) * part_num_nodes;
					end_src_vid = (num_nodes < end_src_vid)? num_nodes: end_src_vid;

					//long io_size = up_aligned((cur_active_ads_idx[nxt_src_pid][end_src_vid-beg_src_vid] * sizeof(ads_entry)), 512);
					long io_size = up_aligned(io_part_ads_sz/2, 512);
					if (ready_buffer == 1){
						long bytes = my_fread(cur_fd_active_ads[nxt_src_pid], new_ads_buffer_2, io_size, 0);
						assert(bytes == io_size);
						nxt_ready_buffer = 2;
					}
					else if (ready_buffer == 2){
						long bytes = my_fread(cur_fd_active_ads[nxt_src_pid], new_ads_buffer_1, io_size, 0);
						assert(bytes == io_size);
						nxt_ready_buffer = 1;
					}
					else{
						bool wrong_ready_buffer_val = false;
						assert(wrong_ready_buffer_val);
					}
				}
				
				//double my_end_tm = wtime();
				//cout << my_end_tm - my_beg_tm << endl;
				
				for(int tid = 0; tid < NUM_THD; ++tid)
					threads[tid].join();
				assert(tasks.is_empty());
			}
			
			//// Write ads_buffer to storage ////
			double tmp_time = wtime();
			io_size = io_part_ads_sz;
			bytes = my_fwrite(fd_ads[dst_pid], ads_buffer, io_size, 0);
			if (debugmode1){
				if (bytes != io_size){
					printf("Unexpected IO error in writing ads_buffer to fd_ads:\n");
					printf("Return bytes: %ld, expected return bytes: %ld\n", bytes, io_size);
				}
			}
			assert(bytes==io_size);
			write_io_time += (wtime() - tmp_time);
			
			
			/*char* char_tmp = (char*)memalign(4096, io_part_ads_sz);
			char* char_ads_buffer = (char*)ads_buffer;
			bytes = my_fread(fd_ads[dst_pid], char_tmp, io_size, 0);
			assert(bytes==io_size);
			for(long i = 0; i < io_part_ads_sz; ++i){
				if (char_tmp[i] != char_ads_buffer[i]){
					cout << dst_pid << ": " << i << " " << io_part_ads_sz << endl;
					fflush(stdout);
				}
				assert(char_tmp[i] == char_ads_buffer[i]);
			}
			free(char_tmp);*/
			
			
			
			//// Write new_ads_buffer to storage ////
			long idx = 0;
			for(unsigned cur_vid = beg_dst_vid; cur_vid < end_dst_vid; ++cur_vid){ // Convert ads_buffer into new_ads_buffer
				unsigned vid_off = cur_vid - beg_dst_vid;
				// Move active ADS-elements from ads_buffer to new_ads_buffer
				/*long beg_bias = vid_off * klogV + all_ads_cnt[dst_pid][vid_off];
				long end_bias = beg_bias + nxt_active_ads_idx[dst_pid][vid_off+1];
				for(long i = beg_bias; i < end_bias; ++i)
					new_ads_buffer[idx++] = ads_buffer[i];*/
				long beg_bias = vid_off * klogV + klogV - 1;
				long end_bias = beg_bias - nxt_active_ads_idx[dst_pid][vid_off+1] + 1;
				for(long i = beg_bias; i >= end_bias; --i){
					new_ads_buffer[idx++] = ads_buffer[i];
				}
			}
			
			for(unsigned vid_off = 0; vid_off < part_num_nodes; ++vid_off){ // Create nxt_active_ads_idx
				all_ads_cnt[dst_pid][vid_off] += nxt_active_ads_idx[dst_pid][vid_off+1];
			}
			
			for(unsigned vid_off = 1; vid_off <= part_num_nodes; ++vid_off){ // Create nxt_active_ads_idx
				nxt_active_ads_idx[dst_pid][vid_off] += nxt_active_ads_idx[dst_pid][vid_off-1];
			}
			assert(idx == nxt_active_ads_idx[dst_pid][end_dst_vid-beg_dst_vid]);
			
			//io_size = ((nxt_active_ads_idx[dst_pid][end_dst_vid-beg_dst_vid] * sizeof(ads_entry)) / 512 + 1) * 512;
			//io_size = up_aligned((nxt_active_ads_idx[dst_pid][end_dst_vid-beg_dst_vid] * sizeof(ads_entry)), (long unsigned int)512);
			io_size = up_aligned(io_part_ads_sz/2, 512);
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
		printf("%.2f, %.2f\n", read_io_time, write_io_time);
		fflush(stdout);
		total_cnt += new_cnt;
	}
	double end_time = wtime();
	cout << "Total Time = " << end_time - beg_time << ", " << total_cnt << endl;
	//cout << total_phase_one_time << " + " << total_phase_two_time << " = " << total_phase_one_time+total_phase_two_time << endl;

	// begin debug
	
	/*unsigned non_zero_cnt = 0;
	long bytes = my_fread(fd_ads[0], ads_buffer, io_part_ads_sz, 0);
	if (debugmode1){
		if (bytes != io_part_ads_sz){
			printf("Unexpected IO error in reading fd_ads into ads_buffer:\n");
			printf("Return bytes: %ld, expected return bytes: %ld\n", bytes, io_part_ads_sz);
		}
	}
	assert(bytes == io_part_ads_sz);
	unsigned show_vid = 1;
	while(true){
		if (all_ads_cnt[0][show_vid] > 0){
			cout << show_vid << ", " << all_ads_cnt[0][show_vid] << endl;
			for(int i = 0; i < all_ads_cnt[0][show_vid]; ++i){
				cout << ads_buffer[show_vid * klogV + i].root_vid << " " << ads_buffer[show_vid * klogV + i].dist << endl;
			}
			fflush(stdout);
			break;
		}
		++show_vid;
		if (show_vid == part_num_nodes){
			cout << 0 << endl;
			fflush(stdout);
			break;
		}
	}
	for(unsigned i = 0; i < part_num_nodes; ++i){
		if (all_ads_cnt[0][i] > 0)
			++non_zero_cnt;
	}
	cout << non_zero_cnt << " / " << part_num_nodes << endl;
	fflush(stdout);*/
	
	
	for(int pid = 0; pid < num_part; ++pid){
		unsigned beg_vid = pid * part_num_nodes;
		unsigned end_vid = (pid+1) * part_num_nodes;
		end_vid = (num_nodes < end_vid)? num_nodes: end_vid;
		
		long bytes = my_fread(fd_ads[pid], ads_buffer, io_part_ads_sz, 0);
		if (debugmode1){
			if (bytes != io_part_ads_sz){
				printf("Unexpected IO error in reading fd_ads into ads_buffer:\n");
				printf("Return bytes: %ld, expected return bytes: %ld\n", bytes, io_part_ads_sz);
			}
		}
		assert(bytes == io_part_ads_sz);
	
		long avg = 0;
		unsigned non_zero_cnt = 0;
		for(unsigned cur_vid = beg_vid; cur_vid < end_vid; ++cur_vid){
			unsigned off_vid = cur_vid - beg_vid;
			if (all_ads_cnt[pid][off_vid] > 100){
				++non_zero_cnt;
			}
			avg += all_ads_cnt[pid][off_vid];
		}

		cout << non_zero_cnt << " / " << (end_vid-beg_vid) << " = " << non_zero_cnt / (double)(end_vid-beg_vid) << endl;
		cout << "Avg: " << avg / (double)(end_vid-beg_vid) << endl;
		fflush(stdout);
	}
	// end debug

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