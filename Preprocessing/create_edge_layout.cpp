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

string input_path;
string output_dir;

long num_nodes;
long num_edges;
int num_part;

inline long file_size(string filename) {
	struct stat st;
	assert(stat(filename.c_str(), &st)==0);
	return st.st_size;
}

void create_edge_layout()
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
	//delete [] indeg;
	free(e_buffer);
	
}

int main(int argc, char** argv)
{
	
	
	return 0;
}