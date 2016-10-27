/*
 * dedup.h
 *
 *  Created on: 2012-7-12
 *      Author: BadBoy
 */

#ifndef DEDUP_H_
#define DEDUP_H_

#include "config.h"
#include "file.h"
#include "disk-hash.h"
#include "cache.h"
#include "data.h"
#include "metadata.h"
#include "sha1.h"
#include "storage-manager.h"
#include "bloom-filter.h"
#ifdef COMPRESS
#include "lzjb.h"
#endif

#include "my-time.h"

#ifdef WRITE_FINGER_TO_FILE
FILE * w_f;
#endif

struct dedup_manager
{
	//the file to store all the data produced by the de-dudplication program
	struct storage_manager manager;

	//the meta-data segment in memory, when it's full, it will be written into disk
	struct mtdata_seg mt_seg;

	//the data segment in memory, when it's full, it will be written into disk
	struct data_seg dt_seg;

	//the data structure to handle the current file to be de-dupliated
	struct file_seg f_seg;
	//the cache for finger-print lookup
	struct cache cache;
	//the disk hash for finger-print lookup
	struct disk_hash disk_hash;

	//the chunk buffer to hold the chunk data
	char chunk_buf[MAX_CHUNK_LEN];

#ifdef COMPRESS
	char compress_buf[MAX_COMPRESS_LEN];
#endif

	//the mem_hash for finger-print lookup
	struct mem_hash mem_hash;

	//the bloom-filter
	char * bf;//char bf[BF_LEN];

	//the struct to store metadata read from disk to cache
//	struct metadata mtdata[MTDATA_PER_SEGMENT];
	struct metadata mtdata[MAX_BEFORE_LEN * 2];
	struct metadata lastmtdata[MAX_BEFORE_LEN * 2];

	struct merge_load_arg merge_load;
};


int dedup_init(struct dedup_manager * dedup, char * data_file_name)
{
	if(NULL == dedup)
		return -1;
	init_power(WIN_LEN);
	storage_manager_init(&(dedup->manager), data_file_name);
	dedup->f_seg.manager = &(dedup->manager);
	dedup->disk_hash.manager = &(dedup->manager);
	dedup->dt_seg.manager = &(dedup->manager);
	dedup->mt_seg.manager = &(dedup->manager);
	if(file_init(&(dedup->f_seg)) < 0)
		return -1;
	if(cache_init(&(dedup->cache)) < 0)
		return -1;
	if(disk_hash_init(&(dedup->disk_hash)) < 0)
		return -1;
	if(mem_hash_init(&(dedup->mem_hash)) < 0)
		return -1;
	dedup->bf = malloc(BF_LEN);
	if(NULL == dedup->bf)
	{
		printf("bf too big");
		exit(0);
	}
	if(bloomfilter_init(dedup->bf) < 0)
		return -1;
	if(data_init(&(dedup->dt_seg)) < 0)
		return -1;
	if(metadata_init(&(dedup->mt_seg)) < 0)
		return -1;
	return 0;
}

int load_cache(struct mtdata_seg * seg, struct metadata * mtdata, uint64_t offset, int before_len, int after_len, struct cache * cache)
{
	int len;
	/*if(before_len > MAX_BEFORE_LEN)
		before_len = MAX_BEFORE_LEN;
	if(after_len > MAX_AFTER_LEN)
		after_len = MAX_AFTER_LEN;*/
	len = get_inseg_metadata(seg, mtdata, offset, before_len, after_len);
//	len = get_metadata(seg, mtdata, offset, before_len, after_len);
//	len = get_seg_after_metadata(seg, mtdata, offset);
//	len = get_seg_metadata(seg, mtdata, offset);
	add_metadata_in_cache(mtdata, len, cache);
	return 0;
}

int load2_cache(struct mtdata_seg * seg, struct metadata * mtdata, uint64_t offset, int before_len, int after_len, struct cache * cache)
{
	int len;
	struct timeval start, end;
	if(before_len > MAX_BEFORE_LEN)
		before_len = MAX_BEFORE_LEN;
	if(after_len > MAX_BEFORE_LEN)
		after_len = MAX_BEFORE_LEN;
	gettimeofday(&start, NULL);
	len = get_metadata(seg, mtdata, offset, before_len, after_len);
	gettimeofday(&end, NULL);
	add2_metadata_in_cache(mtdata, len, cache);
	return 0;
}

int merge_load_cache(struct mtdata_seg * seg, struct metadata * mtdata, struct merge_load_arg * merge_load, struct cache * cache)
{
	uint32_t len;
	struct timeval start, end;
	gettimeofday(&start, NULL);
	len = merge_get_metadata(seg, mtdata, merge_load);
	gettimeofday(&end, NULL);
	clear_new_in_cache(cache);
	add_metadata_in_cache(mtdata, len, cache);
	return 0;
}

int after_identify(struct metadata * mt, char * data, struct dedup_manager * dedup, char dup)
{
	struct timeval start, end;
	struct disk_hash_node node;
	gettimeofday(&start, NULL);
	if(0 == dup)
	{
		unique_len += mt->origin_len;
		this_unique_len += mt->origin_len;
		if(CUR_LINE_TYPE == LINE_TYPE_HICE)
		{
			mt->len = lzjb_compress(data, dedup->compress_buf, mt->len, MAX_COMPRESS_LEN, 0);
			data = dedup->compress_buf;
		}
		mt->offset = add_data(data, mt->len, &dedup->dt_seg);
		node.data_len = mt->len;
		node.mtdata_offset = add_metadata(*mt, &(dedup->mt_seg));
		node.data_offset = mt->offset;
		memcpy(node.fingerprint, mt->fingerprint, FINGERPRINT_LEN);
		add_2_disk_hash(&dedup->disk_hash, node);
	}
	else
	{
		dup_num ++;
		this_dup_num ++;
		dup_len += mt->origin_len;
		this_dup_len += mt->origin_len;
		add_metadata(*mt, &(dedup->mt_seg));
	}
	gettimeofday(&end, NULL);
	store_time += td(&start, &end);
	this_store_time += td(&start, &end);
	return 0;
}

int de_mh_frag(struct mem_hash * mh, struct dedup_manager * dedup)
{
	int i;
	struct list *list, *safe, *result, *free, * inner_dup;
	struct mem_hash_node * mnode, *mnode_dup;
	for(i = 0 ; i < BUCKET_NUM ; i++)
	{
		result = &mh->mem_bucket[i].result;
		free = &mh->mem_bucket[i].free;
		inner_dup = &mh->mem_bucket[i].inner_dup;
		list_iterate_safe(list, safe, result)
		{
			mnode = list_item(list, struct mem_hash_node);
			after_identify(&mnode->mtdata, mnode->data, dedup, mnode->dup);
			list_del(list);
			list_add(free, list);
		}
		list_iterate_safe(list, safe, inner_dup)
		{
			mnode = list_item(list, struct mem_hash_node);
			mnode_dup = mnode->innter_dup;
			if(1 == mnode_dup->result)
			{
				memcpy(&mnode->mtdata, &mnode_dup->mtdata, sizeof(struct metadata));
				after_identify(&mnode->mtdata, mnode->data, dedup, 1);
				list_del(list);
				list_add(free, list);
			}
		}
	}
	return 0;
}

int cache_count[1024];

int line_cache_lookup(struct dedup_manager * dedup, struct mem_hash * mh, struct cache * cache, struct mem_hash_node * mhash_node)
{
	uint32_t first_int, index;
//	uint64_t hit_id;
	int ret, i = 0, j = 0;
	struct mem_hash_node * mnode, *next;
	struct timeval start, end;

//	printf("disk hit id id id id %d\n", mhash_node->mtdata.id);
	mnode = mhash_node->next;
	while((mnode != mhash_node) && (mnode != &mh->head))
	{
//		hit_id = mnode->mtdata.id;
		next = mnode->next;
		gettimeofday(&start, NULL);
		ret = lookup_in_cache(cache, &mnode->mtdata);
		mnode->cache_count ++;
		cache_times ++;
		this_cache_times ++;
		gettimeofday(&end, NULL);
		mem_hash_cache_look_time += td(&start, &end);
		this_mem_hash_cache_look_time += td(&start, &end);
		if(1 == ret)
		{
			memcpy((void *)&first_int, (void*)mnode->mtdata.fingerprint, sizeof(uint32_t));
			index = first_int & DISK_HASH_MASK;
			after_identify(&mnode->mtdata, mnode->data, dedup, 1);
			list_del(&mnode->list);
			list_add(&mh->mem_bucket[index].free, &mnode->list);
			mnode->result = 1;
			mhnode_list_del(mnode);
			mhnode_list_init(mnode);
			j ++;
			second_cache_hit ++;
			this_second_cache_hit ++;
			cache_count[mnode->cache_count] ++;
//			printf("second cache hit metadata id %d\n", hit_id);
		}
		else
		{
			if(mnode->cache_count >= CACHE_THRESHOLD)
			{
				mhnode_list_del(mnode);
				mhnode_list_init(mnode);
			}
		}
		mnode = next;
		i ++;
	}
	if(mnode == &mh->head)
	{
		mnode = mh->head.next;
		while(mnode != mhash_node)
		{
//			hit_id = mnode->mtdata.id;
			next = mnode->next;
			gettimeofday(&start, NULL);
			ret = lookup_in_cache(cache, &mnode->mtdata);
			mnode->cache_count ++;
			cache_times ++;
			this_cache_times ++;
			gettimeofday(&end, NULL);
			mem_hash_cache_look_time += td(&start, &end);
			this_mem_hash_cache_look_time += td(&start, &end);
			if(1 == ret)
			{
				memcpy((void *)&first_int, (void*)mnode->mtdata.fingerprint, sizeof(uint32_t));
				index = first_int & DISK_HASH_MASK;
				after_identify(&mnode->mtdata, mnode->data, dedup, 1);
				memcpy((void *)&first_int, (void*)mnode->mtdata.fingerprint, sizeof(uint32_t));
				index = first_int & DISK_HASH_MASK;
				list_del(&mnode->list);
				list_add(&mh->mem_bucket[index].free, &mnode->list);
				mnode->result = 1;
				mhnode_list_del(mnode);
				mhnode_list_init(mnode);
				j ++;
				second_cache_hit ++;
				this_second_cache_hit ++;
				cache_count[mnode->cache_count] ++;
//				printf("second cache hit metadata id %d\n", hit_id);
			}
			else
			{
				if(mnode->cache_count >= CACHE_THRESHOLD)
				{
					mhnode_list_del(mnode);
					mhnode_list_init(mnode);
				}
			}
			mnode = next;
			i ++;
		}

	}
//	printf("chain id %d, chain offset %d, chain len %d, dup len %d\n", mhash_node->chai_id, mhash_node->chain_offset, i, j);
	return 0;
}

int line_no_metadata_cache_lookup(struct dedup_manager * dedup, struct mtdata_seg * seg, struct metadata * mtdata, struct mem_hash * mh, struct cache * cache, struct mem_hash_node * mhash_node, uint64_t offset, uint32_t before_len)
{
	int len;
	struct timeval start, end;
	before_len += LOAD_ADVANCE;
	gettimeofday(&start, NULL);
//	len = get_metadata(seg, mtdata, offset, before_len, MAX_BEFORE_LEN - before_len + 2*LOAD_ADVANCE);
//	len = get_seg_metadata(seg, mtdata, offset);
//	len = get_seg_before_after_metadata(seg, mtdata, offset, before_len);
	len = get_inseg_metadata(seg, mtdata, offset, before_len, MAX_BEFORE_LEN - before_len + 2*LOAD_ADVANCE);
	gettimeofday(&end, NULL);

	add_metadata_in_cache(mtdata, len, cache);
	gettimeofday(&end, NULL);

	line_cache_lookup(dedup, mh, cache, mhash_node);
	return len;
}

int put_in_mem_hash(struct metadata * mt, char * data, struct mem_hash * mh, struct disk_hash * disk_hash, struct dedup_manager * dedup, int new_chain, int chain_off)
{
	static int chain_id = 0;
	uint32_t first_int, index;
	struct timeval start, end;
	struct mem_hash_node *mnode, *mnode_dup, * last_node;
	uint32_t last_node_index;
	struct merge_load_arg * merge_load;
	struct list * list, *safe, *free;
	int len, i;
	int need_lookup_cache = 0;
	int first_cache = 0;

	memcpy((void *)&first_int, (void*)mt->fingerprint, sizeof(uint32_t));
	index = first_int & DISK_HASH_MASK;
	list = list_first(&mh->mem_bucket[index].free);
	mnode = list_item(list, struct mem_hash_node);
	memcpy(&mnode->mtdata, mt, sizeof(struct metadata));
	memcpy(&mnode->data, data, mt->len);
	mnode->dup = 0;
	mnode->result = 0;
	mnode->chain_offset = chain_off;
	list_del(list);
	list_add(&mh->mem_bucket[index].head, list);
	if(1 == new_chain)
	{
//		printf("list len %d\n", mh_list_size(&mh->head));
		mhnode_list_del(&mh->head);
		mhnode_list_init(&mh->head);
		chain_id ++;
	}
	mnode->chai_id = chain_id;
	mnode->cache_count = 0;
	mhnode_list_add(&mh->head, mnode);
	if(list_empty(&mh->mem_bucket[index].free))
	{
		/*if(FH_LEN != list_size(&mh->mem_bucket[index].head))
		{
			printf("lost mh node\n");
			exit(0);
		}*/
		last_node = mnode;
		gettimeofday(&start, NULL);
		mem_hash_lookup(disk_hash, mh, index);
		gettimeofday(&end, NULL);
		disk_hash_look_time += td(&start, &end);
		this_disk_hash_look_time += td(&start, &end);
		free = &mh->mem_bucket[index].free;

#ifdef MERGE_LOAD_CACHE
		merge_load = &dedup->merge_load;
		merge_load->len = 0;
		need_lookup_cache = 0;

		i = 0;
		list = (&mh->mem_bucket[index].result)->n;
		while(list != &mh->mem_bucket[index].result)
		{
			mnode = list_item(list, struct mem_hash_node);
			if((1 == mnode->dup) && (mnode->next != mnode))
			{
				if(mnode == last_node)
				{
					last_node_index = i;
					first_cache = 1;
				}
//				printf("disk hit id %d\n", mnode->mtdata.id);
				merge_load->node[i].before = mnode->chain_offset + LOAD_ADVANCE;
				merge_load->node[i].offset = mnode->dnode.mtdata_offset;
				merge_load->node[i].after = MAX_BEFORE_LEN - mnode->chain_offset + 2 * LOAD_ADVANCE;
				mnode->load_cache_index = i;
				i ++;
				need_lookup_cache = 1;
			}
#ifndef LINE_LOOKUP
			after_identify(&mnode->mtdata, mnode->data, dedup, mnode->dup);
			safe = list->n;
			list_del(list);
			list_add(free, list);
			mhnode_list_del(mnode);
			list = safe;
#else
			list = list->n;
#endif
		}
		merge_load->len = i;

		if(1 == need_lookup_cache)
		{
			len = merge_get_metadata(&dedup->mt_seg, dedup->mtdata, merge_load);
//			len = merge_get_seg_metadata(&dedup->mt_seg, dedup->mtdata, merge_load);
		}
		gettimeofday(&start, NULL);
#ifdef LINE_LOOKUP
#ifdef SEPERATE_LOAD_CACHE
		list = (&mh->mem_bucket[index].result)->n;
		while(list != &mh->mem_bucket[index].result)
		{
			mnode = list_item(list, struct mem_hash_node);
			after_identify(&mnode->mtdata, mnode->data, dedup, mnode->dup);
			if(1 == mnode->dup)
			{
				if((1 == mnode->dup) && (mnode->next != mnode))
				{
					i = mnode->load_cache_index;
//					printf("disk hit id %d\n", mnode->mtdata.id);
					add_metadata_in_cache(dedup->mtdata + merge_load->index[i].mt_offset, merge_load->index[i].len, &dedup->cache);
					line_cache_lookup(dedup, mh, &dedup->cache, mnode);
				}
			}
			safe = list->n;
			list_del(list);
			list_add(free, list);
			mhnode_list_del(mnode);
			list = safe;
		}
#else
		add_metadata_in_cache(dedup->mtdata, len, &dedup->cache);

		list = (&mh->mem_bucket[index].result)->n;
		while(list != &mh->mem_bucket[index].result)
		{
			mnode = list_item(list, struct mem_hash_node);
			after_identify(&mnode->mtdata, mnode->data, dedup, mnode->dup);
			if((1 == mnode->dup) &&(mnode->next != mnode))
			{
				line_cache_lookup(dedup, mh, &dedup->cache, mnode);
			}
			safe = list->n;
			list_del(list);
			list_add(free, list);
			mhnode_list_del(mnode);
			list = safe;
		}
#endif
#else
		if(1 == need_lookup_cache)
		{
			clear_new_in_cache(&dedup->cache);
			add_metadata_in_cache(dedup->mtdata, len, &dedup->cache);
			lookup_mem_hash_in_cache(&dedup->cache, mh);
			gettimeofday(&end, NULL);
			mem_hash_cache_look_time += td(&start, &end);
			this_mem_hash_cache_look_time += td(&start, &end);
			gettimeofday(&start, NULL);
			de_mh_frag(mh, dedup);
			gettimeofday(&end, NULL);
			defrag_time += td(&start, &end);
			this_defrag_time += td(&start, &end);
		}
#endif
#else
		list = (&mh->mem_bucket[index].result)->n;
		while(list != &mh->mem_bucket[index].result)
		{
			mnode = list_item(list, struct mem_hash_node);
			after_identify(&mnode->mtdata, mnode->data, dedup, mnode->dup);
			cache_count[mnode->cache_count] ++;
			if((1 == mnode->dup) && (mnode->next != mnode))
			{
//				printf("disk hit id %d\n", mnode->mtdata.id);
				len = line_no_metadata_cache_lookup(dedup, &dedup->mt_seg, dedup->mtdata, &dedup->mem_hash, &dedup->cache, mnode,mnode->dnode.mtdata_offset, mnode->chain_offset);
				if((mnode == last_node) || ((NULL != last_node->innter_dup) && (mnode == last_node->innter_dup)))
				{
					first_cache = 1;
					memcpy(dedup->lastmtdata, dedup->mtdata, sizeof(struct metadata) * len);
				}
			}
			safe = list->n;
			list_del(list);
			list_add(free, list);
			mhnode_list_del(mnode);
			list = safe;
		}
#endif
//		exit(0);
		list_iterate_safe(list, safe, &mh->mem_bucket[index].inner_dup)
		{
			mnode = list_item(list, struct mem_hash_node);
			mnode_dup = mnode->innter_dup;
			if((1 == mnode_dup->result) && (1 == mnode_dup->dup))
			{
				memcpy(&mnode->mtdata, &mnode_dup->mtdata, sizeof(struct metadata));
				memcpy(&mnode->dnode, &mnode_dup->dnode, sizeof(struct disk_hash_node));
				after_identify(&mnode->mtdata, mnode->data, dedup, 1);
				list_del(list);
				list_add(free, list);
				mhnode_list_del(mnode);
				mnode->result = 1;
				mnode->dup = 1;
			}
		}
		if(1 == first_cache)
		{
			gettimeofday(&start, NULL);
#ifdef MERGE_LOAD_CACHE
			add_metadata_in_cache(dedup->mtdata + merge_load->index[last_node_index].mt_offset, merge_load->index[last_node_index].len, &dedup->cache);
#else
			add_metadata_in_cache(dedup->lastmtdata, len, &dedup->cache);
#endif
			gettimeofday(&end, NULL);
			mh->first_cache = 1;
		}
		else
		{
			if((1 == last_node->result) && (1 == last_node->dup))
			{
//				len = get_metadata(&dedup->mt_seg, dedup->mtdata, last_node->dnode.mtdata_offset, last_node->chain_offset + LOAD_ADVANCE, TO_CACHE_LEN);
//				len = get_seg_after_metadata(&dedup->mt_seg, dedup->mtdata, last_node->dnode.mtdata_offset);
				len = get_inseg_metadata(&dedup->mt_seg, dedup->mtdata, last_node->dnode.mtdata_offset, 0, MAX_BEFORE_LEN);
				add_metadata_in_cache(dedup->mtdata, len, &dedup->cache);
				mh->first_cache = 1;
			}
		}
	}
	return 0;
}

uint64_t id = 0;

#ifdef FINGER_DEBUG
int dedup(char * buf, uint32_t len, struct dedup_manager * dedup)
{
	return 0;
}
int debug_dedup(struct dedup_manager * dedup, char * finger)
#else
int dedup(char * buf, uint32_t len, struct dedup_manager * dedup)
#endif
{
#ifdef DEBUG
	int ret = 0;
	int need_load_cache;
	static int pre_unique = 0;
	static int chain_off = 0;
	static int unique_count = 0;
	static int dup_count = 0;
	static int new_chain = 1;
	struct metadata mtdata;
	struct disk_hash_node node;
	struct timeval start, end, iden_start, iden_end;
	struct timeval this_time;
#ifdef FINGER_DEBUG
	char * buf;
	int len;
	char buf2[1024];
	buf = buf2;
	len = 1024;
#endif
#endif
	if(id >=1000000)//if(data_len >= next_data_len)
	{
		id = 0;
		gettimeofday(&this_time, NULL);
		this_backup_time = td(&previous_time, &this_time);
		previous_time = this_time;
		next_data_len += STATISTIC_LEN;
		this_identify_time -= this_store_time;
		puts("");
		printf("identify time %f\n", identify_time / 1000000.0);
		printf("disk hash lookup time %f\n", disk_hash_look_time / (1000 * 1000 * 1.0));
		printf("disk hash fread times %d\n", disk_hash_fread_times);
		printf("disk hash fread time %fsec\n", disk_hash_fread_time / (1000 *1000.0));
		printf("cache time is %f\n", cache_time / 1000000.0);
		printf("cache times is %d\n", cache_times);
		printf("load cache time %f\n", load_cache_time / 1000000.0);
		printf("load cache times %d\n", load_cache_times);
		printf("first cache hit time is %d\n", first_cache_hit);
		puts("");
		id = 0;
		/*printf("compress time %f\n", this_compress_time / 1000000.0);
		printf("hash time %f\n", this_hash_time / 1000000.0);
		printf("identify time %f\n", this_identify_time / 1000000.0);
		printf("encrypt time %f\n", this_encrypt_time / 1000000.0);
		printf("store time %f\n", this_store_time / 1000000.0);
		printf("read file time %f\n", this_read_file_time / 1000000.0);
		printf("chunk time %f\n", this_chunk_time / 1000000.0);
		printf("data len %f MB\n", this_data_len / (1024 * 1024 * 1.0));
		printf("compress len %f MB\n", this_compress_len / (1024 * 1024 * 1.0));
		printf("unique len %f MB\n", this_unique_len / (1024 * 1024 * 1.0));
		printf("dup len %f MB\n", this_dup_len / (1024 * 1024 * 1.0));
		printf("mem hash time %f\n", this_mem_hash_time / (1000 * 1000 * 1.0));
		printf("disk hash lookup time %f\n", this_disk_hash_look_time / (1000 * 1000 * 1.0));
		printf("disk hash fread times %d\n", this_disk_hash_fread_times);
		printf("disk hash fread time %fsec\n", this_disk_hash_fread_time / (1000 *1000.0));
		printf("disk hash fread len %fMB\n", this_disk_hash_fread_len / (1024 * 1024.0));
		printf("rank time %f\n", this_rank_time / (1000 * 1000.0));
		printf("rank fread time %f\n", this_rank_fread_time / (1000 * 1000.0));
		printf("defrag time %f\n", this_defrag_time / (1024 * 1024 * 1.0));
		printf("mem hash cache lookup time %f\n", this_mem_hash_cache_look_time / (1024 * 1024 * 1.0));
		printf("backup time %f\n", this_backup_time / 1000000.0);
		printf("backup throughtput is %fMB/s\n", (this_data_len / (1024 * 1024.0)) / (this_backup_time / (1000000.0)));
		printf("cache time is %f\n", this_cache_time / 1000000.0);
		printf("cache times is %d\n", this_cache_times);
		printf("load cache time %f\n", this_load_cache_time / 1000000.0);
		printf("load cache times %d\n", this_load_cache_times);
		printf("load cache fread times %d\n", this_load_cache_read_times);
		printf("load cache fread time %fsec\n", this_load_cache_read_time / (1000 * 1000.0));
		printf("load cache fread len %f MB\n", this_load_cache_read_len / (1024 * 1024.0));
		printf("dup_num is %d\n", this_dup_num);
		printf("bloom filter hit time is %d\n", this_bf_hit);
		printf("first cache hit time is %d\n", this_first_cache_hit);
		printf("second cache hit time is %d\n", this_second_cache_hit);
		printf("disk hash hit time is %d\n", this_disk_hash_hit);
		printf("cache hit ratio is %f\n", (this_first_cache_hit + this_second_cache_hit) * 1.0/ this_dup_num);
		printf("bloom filter time is %f\n", this_bf_time/1000000.0);
		printf("fread time %f\n", this_fread_time / 1000000.0);
		printf("fread times %d\n", this_fread_times);
		printf("fread len %fMB\n", this_fread_len / (1024.0 *1024));
		printf("fwrite time %f\n", this_fwrite_time / 1000000.0);
		printf("fwrite times %d\n", this_fwrite_times);
		printf("fwrite len %fMB\n", this_fwrite_len / (1024.0 *1024));
		printf("data len %ld, chunk num %ld, chunk len %ld\n", this_data_len, this_chunk_num, this_data_len / this_chunk_num);
		printf("total data len %fGB\n\n", data_len / (1024.0 * 1024 * 1024));*/

		this_compress_time = 0;
		this_hash_time = 0;
		this_identify_time = 0;
		this_encrypt_time = 0;
		this_store_time = 0;
		this_chunk_time = 0;
		this_bf_time = 0;
		this_cache_time = 0;
		this_cache_times = 0;
		this_disk_time = 0;
		this_first_cache_hit = 0;
		this_second_cache_hit = 0;
		this_bf_hit = 0;
		this_disk_hash_hit = 0;
		this_dup_num = 0;
		this_mem_hash_time = 0;
		this_disk_hash_look_time = 0;
		this_disk_hash_fread_times = 0;
		this_disk_hash_fread_time = 0;
		this_disk_hash_fread_len = 0;
		this_rank_time = 0;
		this_rank_fread_time = 0;
		this_rank_fwrite_time = 0;
		this_defrag_time = 0;
		this_load_cache_time = 0;
		this_load_cache_times = 0;
		this_load_cache_read_time = 0;
		this_load_cache_read_times = 0;
		this_load_cache_read_len = 0;
		this_mem_hash_cache_look_time = 0;
		this_backup_time = 0;
		this_read_file_time = 0;
		this_fread_time = 0;
		this_fread_times = 0;
		this_fread_len = 0;
		this_fwrite_len = 0;
		this_fwrite_time = 0;
		this_fwrite_times = 0;

		this_data_len = 0;
		this_compress_len = 0;
		this_unique_len = 0;
		this_dup_len = 0;
		this_chunk_num = 0;
	}
	chunk_num ++;
	this_chunk_num ++;
	data_len += len;
	this_data_len += len;

	mtdata.origin_len = len;
	if(CUR_LINE_TYPE == LINE_TYPE_CHIE)
	{
		gettimeofday(&start, NULL);
		mtdata.origin_len = len;
		len = lzjb_compress(buf, dedup->compress_buf, len, MAX_COMPRESS_LEN, 0);
		buf = dedup->compress_buf;
		gettimeofday(&end, NULL);
		compress_time += td(&start, &end);
		this_compress_time += td(&start, &end);
		compress_len += len;
		this_compress_len += len;
	}
	id ++;
#ifdef FINGER_DEBUG
	memcpy(mtdata.fingerprint, finger, FINGERPRINT_LEN);
#else
	if(id % 4096 == 0)
	{
		printf("data len %f MB\n", data_len / (1024 * 1024 * 1.0));
		printf("dup len %f MB\n", dup_len / (1024 * 1024 * 1.0));
		printf("dup_num is %d\n", dup_num);
		puts("");
	}
	gettimeofday(&start, NULL);
	sha1((unsigned char *)buf, len, (unsigned char *)mtdata.fingerprint);
	gettimeofday(&end, NULL);
	hash_time += td(&start, &end);
	this_hash_time += td(&start, &end);
#endif

#ifdef WRITE_FINGER_TO_FILE
	fwrite(mtdata.fingerprint, 1, FINGERPRINT_LEN, w_f);
#endif

	gettimeofday(&iden_start, NULL);
	mtdata.len = len;
	gettimeofday(&start, NULL);
	ret = bloom_filter_lookup(dedup->bf, mtdata.fingerprint);    //bloom filter
	gettimeofday(&end, NULL);
	bf_time += td(&start, &end);
	this_bf_time += td(&start, &end);
#ifdef BATCH_DEDUP
	if(0 == ret)
	{
		after_identify(&mtdata, buf, dedup, ret);
		pre_unique ++;
		if(pre_unique == PRE_UNIQUE_THREHOLD)
		{
			chain_off = 0;
			new_chain = 1;
		}
	}
	else
	{
		bf_hit ++;
		this_bf_hit ++;
		gettimeofday(&start, NULL);
		if(pre_unique <= PRE_UNIQUE_THREHOLD)
		{
			chain_off += pre_unique;
		}
		if(chain_off >= MAX_BEFORE_LEN)
		{
			chain_off = 0;
			new_chain = 1;
		}
		pre_unique = 0;
		if(1 == dedup->mem_hash.first_cache)
		{
			ret = lookup_in_cache(&dedup->cache, &mtdata);
			cache_times ++;
			this_cache_times ++;
			if(1 == ret)
			{
				this_first_cache_hit ++;
				first_cache_hit ++;
				if(dedup->mem_hash.cache_miss > 0)
				{
					dedup->mem_hash.cache_miss --;
				}
			}
			else
			{
				dedup->mem_hash.cache_miss ++;
				if(dedup->mem_hash.cache_miss >= CACHE_MISS_THRESHOLD)
				{
					dedup->mem_hash.cache_miss = 0;
					dedup->mem_hash.first_cache = 0;
				}
			}
		}
		else
		{
			ret = 0;
		}
		gettimeofday(&end, NULL);
		cache_time += td(&start, &end);
		this_cache_time += td(&start, &end);
		if(1 == ret)
		{
			after_identify(&mtdata, buf, dedup, ret);
		}
		else
		{
			gettimeofday(&start, NULL);
			put_in_mem_hash(&mtdata, buf, &dedup->mem_hash, &dedup->disk_hash, dedup, new_chain, chain_off);
			if(1 == new_chain)
			{
				new_chain = 0;
			}
			gettimeofday(&end, NULL);
			mem_hash_time += td(&start, &end);
			this_mem_hash_time += td(&start, &end);
		}
		chain_off ++;
	}
#else

	if(1 == ret)    //possibly duplicated
	{
		bf_hit ++;
		this_bf_hit ++;
		need_load_cache = 0;
		gettimeofday(&start, NULL);
		//ret = lookup2_in_cache(&dedup->cache, mtdata.fingerprint, &mtdata);
		ret = lookup_in_cache(&dedup->cache, &mtdata);    //look up in cache first
		cache_times ++;
		this_cache_times ++;
		gettimeofday(&end, NULL);
		cache_time += td(&start, &end);
		this_cache_time += td(&start, &end);
		if(0 == ret)
		{
			gettimeofday(&start, NULL);
			ret = lookup_fingerprint_in_disk_hash(&dedup->disk_hash, mtdata.fingerprint, &node);
			gettimeofday(&end, NULL);
			disk_hash_look_time+= td(&start, &end);
			this_disk_hash_look_time+= td(&start, &end);
			if(1 == ret)
			{
				disk_hash_hit ++;
				this_disk_hash_hit ++;
				mtdata.offset = node.data_offset;
				need_load_cache = 1;
			}
		}
		else
		{
			first_cache_hit ++;
			this_first_cache_hit ++;
		}
	}
	if(0 == ret)   //unique
	{
		unique_count ++;
		if(dup_count != 0)
		{
//			printf("serial dup len %d\n", dup_count);
			dup_count = 0;
		}
		gettimeofday(&start, NULL);
		unique_len += mtdata.origin_len;
		this_unique_len += mtdata.origin_len;

		if(CUR_LINE_TYPE == LINE_TYPE_HICE)
		{
			mtdata.origin_len = len;
			len = lzjb_compress(buf, dedup->compress_buf, len, MAX_COMPRESS_LEN, 0);
			mtdata.len = len;
			buf = dedup->compress_buf;
		}
		mtdata.offset = add_data(buf, len, &dedup->dt_seg);
		node.data_len = len;
		node.mtdata_offset = add_metadata(mtdata, &(dedup->mt_seg));
		node.data_offset = mtdata.offset;
		memcpy(node.fingerprint, mtdata.fingerprint, FINGERPRINT_LEN);
		add_2_disk_hash(&dedup->disk_hash, node);
		gettimeofday(&end, NULL);
		store_time += td(&start, &end);
		this_store_time += td(&start, &end);
	}
	else
	{
		dup_count ++;
		if(unique_count != 0)
		{
//			printf("serial unique len %d\n", unique_count);
			unique_count = 0;
		}
		dup_num ++;
		this_dup_num ++;
		dup_len += mtdata.origin_len;
		gettimeofday(&start, NULL);
		this_dup_len += mtdata.origin_len;
		add_metadata(mtdata, &(dedup->mt_seg));
		gettimeofday(&end, NULL);
		store_time += td(&start, &end);
		this_store_time += td(&start, &end);
		if(need_load_cache)
		{
			//clear2_new_in_cache(&dedup->cache);
			//load2_cache(&(dedup->mt_seg), dedup->mtdata, node.mtdata_offset, 0, TO_CACHE_LEN, &dedup->cache);
			load_cache(&(dedup->mt_seg), dedup->mtdata, node.mtdata_offset, 0, MAX_BEFORE_LEN, &dedup->cache);
		}
	}
#endif
	gettimeofday(&iden_end, NULL);
	identify_time += td(&iden_start, &iden_end);
	this_identify_time += td(&iden_start, &iden_end);
	return ret;
}

int destroy(struct dedup_manager * dedup)
{
	fclose(dedup->manager.f);
	return 0;
}

#endif /* DEDUP_H_ */
