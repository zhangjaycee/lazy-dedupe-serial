/*
 * my-time.h
 *
 *  Created on: 2012-11-8
 *      Author: badboy
 */

#ifndef MY_TIME_H_
#define MY_TIME_H_

#include "config.h"

#ifdef DEBUG
uint64_t compress_time = 0;
uint64_t hash_time = 0;
uint64_t identify_time = 0;
uint64_t encrypt_time = 0;
uint64_t store_time = 0;
uint64_t chunk_time = 0;
uint64_t bf_time = 0;
uint64_t cache_time = 0;
uint64_t cache_times = 0;
uint64_t disk_time = 0;
uint64_t first_cache_hit = 0;
uint64_t second_cache_hit = 0;
uint64_t bf_hit = 0;
uint64_t disk_hash_hit = 0;
uint64_t dup_num = 0;
uint64_t mem_hash_time = 0;
uint64_t disk_hash_look_time = 0;
uint64_t disk_hash_fread_times = 0;
uint64_t disk_hash_fread_time = 0;
uint64_t disk_hash_fread_len = 0;
uint64_t rank_time = 0;
uint64_t rank_fread_time = 0;
uint64_t rank_fwrite_time = 0;
uint64_t defrag_time = 0;
uint64_t load_cache_time = 0;
uint64_t load_cache_times = 0;
uint64_t load_cache_read_time = 0;
uint64_t load_cache_read_times = 0;
uint64_t load_cache_read_len = 0;
uint64_t mem_hash_cache_look_time = 0;
uint64_t backup_time = 0;
uint64_t read_file_time = 0;
uint64_t fread_time = 0;
uint64_t fread_times = 0;
uint64_t fread_len = 0;
uint64_t fwrite_len = 0;
uint64_t fwrite_time = 0;
uint64_t fwrite_times = 0;
uint64_t continue_dup_len = 0;
uint64_t continue_unique_len = 0;
uint64_t chunk_num = 0;

uint64_t this_compress_time = 0;
uint64_t this_hash_time = 0;
uint64_t this_identify_time = 0;
uint64_t this_encrypt_time = 0;
uint64_t this_store_time = 0;
uint64_t this_chunk_time = 0;
uint64_t this_bf_time = 0;
uint64_t this_cache_time = 0;
uint64_t this_cache_times = 0;
uint64_t this_disk_time = 0;
uint64_t this_first_cache_hit = 0;
uint64_t this_second_cache_hit = 0;
uint64_t this_bf_hit = 0;
uint64_t this_disk_hash_hit = 0;
uint64_t this_dup_num = 0;
uint64_t this_mem_hash_time = 0;
uint64_t this_disk_hash_look_time = 0;
uint64_t this_disk_hash_fread_times = 0;
uint64_t this_disk_hash_fread_time = 0;
uint64_t this_disk_hash_fread_len = 0;
uint64_t this_rank_time = 0;
uint64_t this_rank_fread_time = 0;
uint64_t this_rank_fwrite_time = 0;
uint64_t this_defrag_time = 0;
uint64_t this_load_cache_time = 0;
uint64_t this_load_cache_times = 0;
uint64_t this_load_cache_read_time = 0;
uint64_t this_load_cache_read_times = 0;
uint64_t this_load_cache_read_len = 0;
uint64_t this_mem_hash_cache_look_time = 0;
uint64_t this_backup_time = 0;
uint64_t this_read_file_time = 0;
uint64_t this_fread_time = 0;
uint64_t this_fread_times = 0;
uint64_t this_fread_len = 0;
uint64_t this_fwrite_len = 0;
uint64_t this_fwrite_time = 0;
uint64_t this_fwrite_times = 0;

uint64_t this_data_len = 0;
uint64_t this_compress_len = 0;
uint64_t this_unique_len = 0;
uint64_t this_dup_len = 0;
uint64_t this_chunk_num = 0;

uint64_t data_len = 0;
uint64_t compress_len = 0;
uint64_t unique_len = 0;
uint64_t dup_len = 0;

uint64_t next_data_len = STATISTIC_LEN;
struct timeval previous_time;
#endif

unsigned long long td( struct timeval *t1, struct timeval *t2 ) {
        unsigned long long dt = t2->tv_sec * 1000000 + t2->tv_usec;
        return dt - t1->tv_sec * 1000000 - t1->tv_usec;
}


#endif /* MY_TIME_H_ */
