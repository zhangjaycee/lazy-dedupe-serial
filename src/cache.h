/*
 * cache.h
 *
 *  Created on: 2011-7-19
 *      Author: badboy
 */

#ifndef CACHE_H_
#define CACHE_H_

#include <stdint.h>
#include "config.h"
#include "list.h"
#include "metadata.h"

#ifdef DEBUG
extern uint64_t second_cache_hit;
extern uint64_t first_cache_hit;
extern uint64_t this_second_cache_hit;
extern uint64_t this_first_cache_hit;
#endif

struct cache_node
{
	struct list list;
	struct metadata mtdata;
};

struct cache_bucket
{
	uint32_t len;
	uint32_t new_node;
	uint32_t new_node2;
	struct list list;
	struct cache_node cache_node[CACHE_BUCKET_LEN];
};

struct cache
{
	struct cache_bucket cache_bucket[CACHE_LEN];
};

int cache_init(struct cache * cache)
{
	int i;
	if(NULL == cache)
		return -1;
	for(i = 0 ; i < CACHE_LEN ; i ++)
	{
		cache->cache_bucket[i].len = 0;
		cache->cache_bucket[i].new_node = 0;
		list_init(&cache->cache_bucket[i].list);
	}
	return 0;
}

int clear_new_in_cache(struct cache * cache)
{
	int i;
	for(i = 0 ; i < CACHE_LEN ; i ++)
	{
		cache->cache_bucket[i].new_node = 0;
	}
	return 0;
}

int clear2_new_in_cache(struct cache * cache)
{
	int i;
	for(i = 0 ; i < CACHE_LEN ; i ++)
	{
		cache->cache_bucket[i].new_node2 = 0;
	}
	return 0;
}

int add2_metadata_in_cache(struct metadata * mtdata, size_t len, struct cache * cache)
{
	uint32_t index, pos;
	struct cache_node * cache_node;
	uint32_t i;
	uint32_t first_int;
	struct list * list;

	for(i = 0 ; i < len ; i ++)
	{
		memcpy((void *)&first_int, (void *)mtdata[i].fingerprint, (size_t) sizeof(uint32_t));
		index = first_int & CACHE_MASK;
		pos = cache->cache_bucket[index].len;
		if(pos < CACHE_BUCKET_LEN)
		{
			memcpy(&(cache->cache_bucket[index].cache_node[pos].mtdata), &mtdata[i], sizeof(struct metadata));
			list_add(&cache->cache_bucket[index].list, &cache->cache_bucket[index].cache_node[pos].list);
			cache->cache_bucket[index].len ++;
		}
		else
		{
			list = list_first(&cache->cache_bucket[index].list);
			cache_node = list_item(list, struct cache_node);
			memcpy(&(cache_node->mtdata), &mtdata[i], sizeof(struct metadata));
			list_move(&cache->cache_bucket[index].list, list);
		}
		cache->cache_bucket[index].new_node2 ++;
	}
	return 0;
}

int lookup2_in_cache(struct cache * cache, char fingerprint[FINGERPRINT_LEN], struct metadata * mtdata)
{
	unsigned long index;
	struct list * list, *head;
	struct cache_node * cache_node;
	uint32_t first_int;
	int k;

	memcpy(&first_int, fingerprint, sizeof(uint32_t));
	index = first_int & CACHE_MASK;
	head = &cache->cache_bucket[index].list;
	k = 0;
	list_uniterate(list, head, head)
	{
		cache_node = list_item(list, struct cache_node);
		if(0 == memcmp(cache_node->mtdata.fingerprint, fingerprint, FINGERPRINT_LEN))
		{
			first_cache_hit ++;
			this_first_cache_hit ++;
			list_move(head, list);
			memcpy(mtdata, &(cache_node->mtdata), sizeof(struct metadata));
			return 1;
		}
		k ++;
		if(k >= cache->cache_bucket[index].new_node2)
		{
			return 0;
		}
	}
	return 0;
}

int add_metadata_in_cache(struct metadata * mtdata, size_t len, struct cache * cache)
{
	uint32_t index, pos;
	struct cache_node * cache_node;
	uint32_t i;
	uint32_t first_int;
	struct list * list;

	for(i = 0 ; i < len ; i ++)
	{
		memcpy((void *)&first_int, (void *)mtdata[i].fingerprint, (size_t) sizeof(uint32_t));
		index = first_int & CACHE_MASK;
		pos = cache->cache_bucket[index].len;
		if(pos < CACHE_BUCKET_LEN)
		{
			memcpy(&(cache->cache_bucket[index].cache_node[pos].mtdata), &mtdata[i], sizeof(struct metadata));
			list_add(&cache->cache_bucket[index].list, &cache->cache_bucket[index].cache_node[pos].list);
			cache->cache_bucket[index].len ++;
		}
		else
		{
			list = list_first(&cache->cache_bucket[index].list);
			cache_node = list_item(list, struct cache_node);
			memcpy(&(cache_node->mtdata), &mtdata[i], sizeof(struct metadata));
			list_move(&cache->cache_bucket[index].list, list);
		}
		cache->cache_bucket[index].new_node ++;
	}
	return 0;
}

int lookup_in_cache(struct cache * cache, struct metadata * mtdata)
{
	unsigned long index;
	struct list * list, *head;
	struct cache_node * cache_node;
	uint32_t first_int;

	memcpy(&first_int, mtdata->fingerprint, sizeof(uint32_t));
	index = first_int & CACHE_MASK;  // &0x7ff , last 11 bit as hash index, because there is (2^11 = 2048) cache_bucket
	head = &(cache->cache_bucket[index].list);
	list_uniterate(list, head, head)
	{
		cache_node = list_item(list, struct cache_node);
		if(0 == memcmp(cache_node->mtdata.fingerprint, mtdata->fingerprint, FINGERPRINT_LEN))
		{
			list_move(head, list);
			memcpy(mtdata, &(cache_node->mtdata), sizeof(struct metadata));
			return 1;
		}
	}
	return 0;
}

int lookup_mem_hash_in_cache(struct cache * cache, struct mem_hash * mh)
{
	unsigned long index;
	struct list * list, *head;
	struct list * mlist, *mhead, *safe, * mresult;
	struct mem_hash_node * mnode;
	struct cache_node * cache_node;
	uint32_t first_int;
	int i, k;

	for(i = 0; i < BUCKET_NUM ; i++)
	{
		mhead = &mh->mem_bucket[i].head;
		mresult = &mh->mem_bucket[i].result;
		list_iterate_safe(mlist, safe, mhead)
		{
			mnode = list_item(mlist, struct mem_hash_node);
			k = 0;
			memcpy(&first_int, mnode->mtdata.fingerprint, sizeof(uint32_t));
			index = first_int & CACHE_MASK;
			head = &cache->cache_bucket[index].list;
			list_uniterate(list, head, head)
			{
				cache_node = list_item(list, struct cache_node);
				if(0 == memcmp(cache_node->mtdata.fingerprint, mnode->mtdata.fingerprint, FINGERPRINT_LEN))
				{
//					printf("hit chain %d, chain off %d\n", mh->mem_bucket[i].chain_id[j], mh->mem_bucket[i].chain_off[j]);
					second_cache_hit ++;
					this_second_cache_hit ++;
					list_move(head, list);
					memcpy(mnode, &(cache_node->mtdata), sizeof(struct metadata));
					list_del(mlist);
					list_add(mresult, mlist);
					mnode->dup = 1;
					mnode->result = 1;
					break;
				}
				k ++;
				if(k >= cache->cache_bucket[index].new_node)
				{
					break;
				}
			}
		}
	}
	return 0;
}

#endif /* CACHE_H_ */
