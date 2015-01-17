/*
 * metadata.h
 *
 *  Created on: 2012-7-12
 *      Author: BadBoy
 */

#ifndef METADATA_H_
#define METADATA_H_

#include <stdint.h>
#include "config.h"
#include "storage-manager.h"

struct mtdata_seg_header
{
	uint64_t previous;
	uint64_t next;
};


struct metadata
{
	char fingerprint[FINGERPRINT_LEN];
	uint32_t len;
#ifdef COMPRESS
	uint32_t origin_len;
#endif
	uint64_t offset;
};

#define MTDATA_PER_SEGMENT ((SEG_SIZE - sizeof(struct mtdata_seg_header)) / sizeof(struct metadata))

struct load_node
{
	uint64_t offset;
	uint64_t seg_offset;
	uint32_t before_seg;
	uint32_t after_seg;
	uint32_t before;
	uint32_t after;
	uint32_t from;
	uint32_t to;
	uint32_t rindex;
};

struct load_index
{
	uint32_t mt_offset;
	uint32_t len;
	uint32_t node_index;
	uint32_t merge_index;
};

struct merge_load_arg
{
	struct load_node node[FH_LEN];
	struct load_index index[FH_LEN];
	uint32_t len;
};

struct merged_result
{
	uint64_t seg_offset;
	uint32_t from[FH_LEN];
	uint32_t to[FH_LEN];
	uint32_t valid[FH_LEN];
	uint32_t before_seg;
	uint32_t after_seg;
	uint32_t seg_from;
	uint32_t seg_to;
	uint32_t len;
};

struct merge_load_struct
{
	struct merged_result merge[FH_LEN];
	int len;
};

struct mtdata_seg
{
	struct mtdata_seg_header header;
	//uint64_t mtdata_offset;
	uint64_t mt_seg_offset;
	uint32_t len;
	struct storage_manager * manager;
	struct metadata mtdata[MTDATA_PER_SEGMENT];
	char mtdata_seg_buf[SEG_SIZE];
};

int metadata_init(struct mtdata_seg * mt_seg)
{
	mt_seg->mt_seg_offset = get_new_seg(mt_seg->manager);
	mt_seg->len = 0;
	mt_seg->header.next = 0X00;
	mt_seg->header.previous = 0XFFFFFFFFFFFFFFFF;
	return 0;
}

uint64_t add_metadata(struct metadata mtdata, struct mtdata_seg * mt_seg)
{
	uint64_t offset;
	uint64_t write_offset;
	if(mt_seg->len >= MTDATA_PER_SEGMENT)
	{
		write_offset = mt_seg->mt_seg_offset;
		mt_seg->mt_seg_offset = get_new_seg(mt_seg->manager);
		mt_seg->header.next = mt_seg->mt_seg_offset;
		memcpy(mt_seg->mtdata_seg_buf, &mt_seg->header, sizeof(struct mtdata_seg_header));//simplewrite(write_offset, &mt_seg->header, sizeof(struct mtdata_seg_header), mt_seg->manager);
		mt_seg->header.previous = write_offset;
		//write_offset += sizeof(struct mtdata_seg_header);
		memcpy(mt_seg->mtdata_seg_buf + sizeof(struct mtdata_seg_header), mt_seg->mtdata, MTDATA_PER_SEGMENT * sizeof(struct metadata));//simplewrite(write_offset, mt_seg->mtdata, MTDATA_PER_SEGMENT * sizeof(struct metadata), mt_seg->manager);
		simplewrite(write_offset, mt_seg->mtdata_seg_buf, SEG_SIZE, mt_seg->manager);
		mt_seg->len = 0;
	}
	offset = mt_seg->mt_seg_offset + sizeof(struct mtdata_seg_header) + mt_seg->len * sizeof(struct metadata);
	mt_seg->mtdata[mt_seg->len] = mtdata;
	mt_seg->len ++;
	return offset;
}

int get_seg_metadata(struct mtdata_seg * mt_seg, struct metadata * mtdata, uint64_t offset)
{
	uint64_t seg_offset;
	struct timeval start, end;
	if(offset > mt_seg->mt_seg_offset)
	{
		memcpy(mtdata, mt_seg->mtdata, mt_seg->len * sizeof(struct metadata));
		return mt_seg->len;
	}
	else
	{
		seg_offset = offset / SEG_SIZE * SEG_SIZE;
		gettimeofday(&start, NULL);
		simpleread(seg_offset + sizeof(struct mtdata_seg_header), mtdata, MTDATA_PER_SEGMENT * sizeof(struct metadata), mt_seg->manager);
		gettimeofday(&end, NULL);
		load_cache_read_times ++;
		this_load_cache_read_times ++;
		this_load_cache_read_time += td(&start, &end);
		load_cache_read_time += td(&start, &end);
		this_load_cache_read_len += SEG_SIZE;
		load_cache_read_len += SEG_SIZE;
		return MTDATA_PER_SEGMENT;
	}
}

int get_inseg_metadata(struct mtdata_seg * mt_seg, struct metadata * mtdata, uint64_t offset, int before_len, int after_len)
{
	uint64_t seg_offset;
	struct timeval start, end;
	int before_inseg, after_inseg;
	int start_pos, len;
	if(offset > mt_seg->mt_seg_offset)
	{
		before_inseg = (offset - mt_seg->mt_seg_offset - sizeof(struct mtdata_seg_header)) / sizeof(struct metadata);
		after_inseg = mt_seg->len - before_inseg;
		if(before_len > before_inseg)
		{
			start_pos = 0;
		}
		else
		{
			start_pos = before_inseg - before_len;
		}
		if(after_len > after_inseg)
		{
			len = mt_seg->len - start_pos;
		}
		else
		{
			len = after_len + before_inseg - start_pos;
		}
		memcpy(mtdata, &mt_seg->mtdata[start_pos], len * sizeof(struct metadata));
	}
	else
	{
		seg_offset = offset / SEG_SIZE * SEG_SIZE;
		before_inseg = (offset - seg_offset - sizeof(struct mtdata_seg_header)) / sizeof(struct metadata);
		after_inseg = MTDATA_PER_SEGMENT - before_inseg;
		if(before_len > before_inseg)
		{
			start_pos = 0;
		}
		else
		{
			start_pos = before_inseg - before_len;
		}
		if(after_len > after_inseg)
		{
			len = MTDATA_PER_SEGMENT - start_pos;
		}
		else
		{
			len = after_len + before_inseg - start_pos;
		}
		gettimeofday(&start, NULL);
		simpleread(seg_offset + sizeof(struct mtdata_seg_header) + start_pos * sizeof(struct metadata), mtdata, len * sizeof(struct metadata), mt_seg->manager);
		gettimeofday(&end, NULL);
		load_cache_read_times ++;
		this_load_cache_read_times ++;
		this_load_cache_read_time += td(&start, &end);
		load_cache_read_time += td(&start, &end);
		this_load_cache_read_len += len * sizeof(struct metadata);
		load_cache_read_len += len * sizeof(struct metadata);
	}
	return len;
}

int get_seg_after_metadata(struct mtdata_seg * mt_seg, struct metadata * mtdata, uint64_t offset)
{
	uint64_t seg_offset;
	struct timeval start, end;
	int diff, after_len;
	if(offset > mt_seg->mt_seg_offset)
	{
		diff = (offset - mt_seg->mt_seg_offset - sizeof(struct mtdata_seg_header)) / sizeof(struct metadata);
		after_len = mt_seg->len - diff;
		memcpy(mtdata, &(mt_seg->mtdata[diff]), after_len * sizeof(struct metadata));
	}
	else
	{
		seg_offset = offset / SEG_SIZE * SEG_SIZE;
		diff = (offset - seg_offset - sizeof(struct mtdata_seg_header)) / sizeof(struct metadata);
		after_len = MTDATA_PER_SEGMENT - diff;
		gettimeofday(&start, NULL);
		simpleread(seg_offset + sizeof(struct mtdata_seg_header) + diff * sizeof(struct metadata), mtdata, after_len * sizeof(struct metadata), mt_seg->manager);
		gettimeofday(&end, NULL);
		load_cache_read_times ++;
		this_load_cache_read_times ++;
		this_load_cache_read_time += td(&start, &end);
		load_cache_read_time += td(&start, &end);
		this_load_cache_read_len += after_len * sizeof(struct metadata);
		load_cache_read_len += after_len * sizeof(struct metadata);
	}
	return after_len;
}

int get_seg_before_after_metadata(struct mtdata_seg * mt_seg, struct metadata * mtdata, uint64_t offset, int before_len)
{
	uint64_t seg_offset;
	uint64_t pre_seg_offset;
	uint64_t pre_offset;
	struct mtdata_seg_header * head;
	char read_buf[SEG_SIZE];
	int cpy_len;
	struct timeval start, end;
	int diff, after_len;
	cpy_len = 0;
	if(offset > mt_seg->mt_seg_offset)
	{
		diff = (offset - mt_seg->mt_seg_offset - sizeof(struct mtdata_seg_header)) / sizeof(struct metadata);
		after_len = mt_seg->len - diff;
		if(before_len <= diff)
		{
			memcpy(mtdata, &(mt_seg->mtdata[diff- before_len]), (after_len + before_len) * sizeof(struct metadata));
			before_len -= before_len;
			cpy_len += before_len + after_len;
		}
		else
		{
			memcpy(mtdata, mt_seg->mtdata, mt_seg->len * sizeof(struct metadata));
			cpy_len += mt_seg->len;
			mtdata += mt_seg->len;
			before_len -= diff;
			pre_seg_offset = mt_seg->header.previous;
		}
	}
	else
	{
		seg_offset = offset / SEG_SIZE * SEG_SIZE;
		diff = (offset - seg_offset - sizeof(struct mtdata_seg_header)) / sizeof(struct metadata);
		after_len = MTDATA_PER_SEGMENT - diff;
		if(before_len > diff)
		{
			gettimeofday(&start, NULL);
			simpleread(seg_offset, read_buf, SEG_SIZE, mt_seg->manager);
			memcpy(mtdata, read_buf + sizeof(struct mtdata_seg_header), MTDATA_PER_SEGMENT * sizeof(struct metadata));
			gettimeofday(&end, NULL);
			cpy_len += MTDATA_PER_SEGMENT;
			mtdata += MTDATA_PER_SEGMENT;
			before_len -= diff;
			head = read_buf;
			pre_seg_offset = head->previous;
		}
		else
		{
			gettimeofday(&start, NULL);
			simpleread(seg_offset + sizeof(struct mtdata_seg_header) + (diff - before_len) * sizeof(struct metadata), mtdata, (before_len + after_len) * sizeof(struct metadata), mt_seg->manager);
			gettimeofday(&end, NULL);
			before_len -= before_len;
			cpy_len += before_len + after_len;
		}
		load_cache_read_times ++;
		this_load_cache_read_times ++;
		this_load_cache_read_time += td(&start, &end);
		load_cache_read_time += td(&start, &end);
		this_load_cache_read_len += after_len * sizeof(struct metadata);
		load_cache_read_len += after_len * sizeof(struct metadata);
	}
	if((before_len > 0) && (pre_seg_offset != 0XFFFFFFFFFFFFFFFF))
	{
		if(before_len > MTDATA_PER_SEGMENT)
		{
			pre_offset = pre_seg_offset + sizeof(struct mtdata_seg_header);
			before_len -= MTDATA_PER_SEGMENT;
		}
		else
		{
			pre_offset = pre_seg_offset + sizeof(struct mtdata_seg_header) + (MTDATA_PER_SEGMENT - before_len) * sizeof(struct metadata);
			before_len -= before_len;
		}
		cpy_len += get_seg_before_after_metadata(mt_seg, mtdata, pre_offset, before_len);
	}
	return cpy_len;
}

int get_metadata(struct mtdata_seg * mt_seg, struct metadata * mtdata, uint64_t offset, int before_len, int after_len)
{
	uint32_t diff;
	struct mtdata_seg_header * header;
	uint64_t seg_offset;
	struct timeval start, end;
	int i;
	int ret;
	char read_buf[SEG_SIZE];
	uint64_t read_offset;
	uint32_t read_len;
	uint32_t read_after_len;
	uint32_t read_before_len;
	uint32_t left;
	uint32_t right;
	uint32_t cpy_len;
	cpy_len = 0;
	if(offset > mt_seg->mt_seg_offset)
	{
		diff = (offset - mt_seg->mt_seg_offset - sizeof(struct mtdata_seg_header)) / sizeof(struct metadata);
		if(after_len + diff <= mt_seg->len)
		{
			memcpy(mtdata + before_len, &(mt_seg->mtdata[diff]), after_len * sizeof(struct metadata));
			cpy_len += after_len;
		}
		else
		{
			memcpy(mtdata + before_len, &(mt_seg->mtdata[diff]), (mt_seg->len - diff) * sizeof(struct metadata));
			cpy_len += (mt_seg->len - diff);
		}
		after_len -= after_len;
		if(diff >= before_len)
		{
			memcpy(mtdata, &(mt_seg->mtdata[diff - before_len]), before_len * sizeof(struct metadata));
			cpy_len += before_len;
			before_len -= before_len;
		}
		else
		{
			memcpy(mtdata + (before_len - diff), mt_seg->mtdata, diff * sizeof(struct metadata));
			cpy_len += diff;
			before_len -= diff;
		}
		if(before_len >= MTDATA_PER_SEGMENT)
		{
			offset = mt_seg->header.previous + sizeof(struct mtdata_seg_header);
			before_len -= MTDATA_PER_SEGMENT;
			after_len += MTDATA_PER_SEGMENT;
		}
		else
		{
			offset = mt_seg->header.previous + SEG_SIZE - before_len * sizeof(struct metadata);
			after_len += before_len;
			before_len -= before_len;
		}
	}
	if(after_len + before_len > 0)
	{
		seg_offset = offset / SEG_SIZE * SEG_SIZE;
		right = (seg_offset + SEG_SIZE - offset) / sizeof(struct metadata);
		left = (offset - seg_offset - sizeof(struct mtdata_seg_header)) / sizeof(struct metadata);
		if((before_len > left) || (after_len > right))
		{
			read_offset = seg_offset;
			if(right >= after_len)
			{
				read_len = offset - seg_offset + (after_len * sizeof(struct metadata));
				read_after_len = after_len;
			}
			else
			{
				read_len = SEG_SIZE;
				read_after_len = (seg_offset + SEG_SIZE - offset) / sizeof(struct metadata);
			}
			gettimeofday(&start, NULL);
			simpleread(read_offset, read_buf, read_len, mt_seg->manager);
			gettimeofday(&end, NULL);
			load_cache_read_times ++;
			this_load_cache_read_times ++;
			load_cache_read_time += td(&start, &end);
			this_load_cache_read_time += td(&start, &end);
			this_load_cache_read_len += read_len;
			load_cache_read_len += read_len;
			if(before_len <= left)
			{
				diff = left - before_len;
				read_before_len = before_len;
			}
			else
			{
				diff = 0;
				read_before_len = (offset - seg_offset - sizeof(struct mtdata_seg_header)) / sizeof(struct metadata);
			}
			memcpy(mtdata + (before_len - read_before_len), read_buf + sizeof(struct mtdata_seg_header) + diff * sizeof(struct metadata), (read_before_len + read_after_len) * sizeof(struct metadata));
			cpy_len += (read_before_len + read_after_len);
			header = read_buf;
			after_len -= read_after_len;
			if(after_len > 0)
			{
				ret = get_metadata(mt_seg, mtdata + before_len + read_after_len, header->next + sizeof(struct mtdata_seg_header), 0, after_len);
				cpy_len += ret;
			}
			after_len = 0;
			before_len -= read_before_len;
			if(before_len > 0)
			{
				if(0XFFFFFFFFFFFFFFFF != header->previous)
				{
					if(before_len >= MTDATA_PER_SEGMENT)
					{
						offset = header->previous + sizeof(struct mtdata_seg_header);
						before_len -= MTDATA_PER_SEGMENT;
						after_len += MTDATA_PER_SEGMENT;
					}
					else
					{
						offset = header->previous + SEG_SIZE - before_len * sizeof(struct metadata);
						after_len += before_len;
						before_len -= before_len;
					}
					ret = get_metadata(mt_seg, mtdata, offset, before_len, after_len);
					cpy_len +=  ret;
					if(ret < (before_len + after_len))
					{
						diff = before_len + after_len - ret;
						for(i = 0 ; i < cpy_len ; i ++)
						{
							mtdata[i] = mtdata[i + diff];
						}
					}
				}
				else
				{
					for(i = 0 ; i < cpy_len ; i ++)
					{
						mtdata[i] = mtdata[i + before_len];
					}
				}
			}
		}
		else
		{
			read_len = (before_len + after_len) * sizeof(struct metadata);
			read_offset = offset - sizeof(struct metadata) * before_len;
			gettimeofday(&start, NULL);
			simpleread(read_offset, mtdata, read_len, mt_seg->manager);
			gettimeofday(&end, NULL);
			load_cache_read_times ++;
			this_load_cache_read_times ++;
			this_load_cache_read_time += td(&start, &end);
			load_cache_read_time += td(&start, &end);
			this_load_cache_read_len += read_len;
			load_cache_read_len += read_len;
			cpy_len += (before_len + after_len);
		}
	}
	return cpy_len;
}

int fill_attributes_of_load_node(struct load_node * node, int len)
{
	int i;
	uint32_t seg_before, seg_after;
	for(i = 0 ; i < len ; i ++)
	{
		node[i].seg_offset = node[i].offset / SEG_SIZE * SEG_SIZE;
		seg_before = (node[i].offset - node[i].seg_offset - sizeof(struct mtdata_seg_header)) / sizeof(struct metadata);
		seg_after = (node[i].seg_offset + SEG_SIZE - node[i].offset) / sizeof(struct metadata);
		if(seg_before >= node[i].before)
		{
			node[i].before_seg = 0;
			node[i].from = seg_before - node[i].before;
		}
		else
		{
			node[i].before_seg = node[i].before - seg_before;
			node[i].from = 0;
		}
		if(seg_after >= node[i].after)
		{
			node[i].after_seg = 0;
			node[i].to = node[i].from + (node[i].before - node[i].before_seg + node[i].after);
		}
		else
		{
			node[i].after_seg = node[i].after - seg_after;
			node[i].to = MTDATA_PER_SEGMENT;
		}
	}
	return len;
}

int rank_load_node(struct load_node * node, struct load_index * index, int len)
{
	int i, j;
	uint32_t tmpindex;
	struct load_node tmp;
	for(i = 0 ; i < len ; i ++)
	{
		index[i].node_index = i;
		index[i].mt_offset = 0;
		index[i].len = 0;
		node[i].rindex = i;
	}
	for(i = 0 ; i < len ; i ++)
	{
		for(j = i + 1 ; j < len ; j ++)
		{
			if(node[i].offset > node[j].offset)
			{
				tmp = node[i];
				node[i] = node[j];
				node[j] =tmp;

				tmpindex = index[node[i].rindex].node_index;
				index[node[i].rindex].node_index = index[node[j].rindex].node_index;
				index[node[j].rindex].node_index = tmpindex;
			}
		}
	}
	return len;
}

int merge_node(struct load_node node, struct merged_result * merge)
{
	int merged;
	int i, j, k, merge_index;
	int from, to;
	merged = 0;
	for(i = 0 ; i < merge->len ; i ++)
	{
		if((node.from <= merge->to[i]) && (node.to >= merge->from[i]))
		{
			if(1 == merged)
			{
				if(merge->from[merge_index] > merge->from[i])
				{
					merge->from[merge_index] = merge->from[i];
				}
				if(merge->to[merge_index] < merge->to[i])
				{
					merge->to[merge_index] = merge->to[i];
				}
				merge->valid[i] = 0;
			}
			else
			{
				if(merge->from[i] > node.from)
				{
					merge->from[i] = node.from;
				}
				if(merge->to[i] < node.to)
				{
					merge->to[i] = node.to;
				}
				merged = 1;
				merge_index = i;
			}
		}
	}
	if(0 == merged)
	{
		merge->from[merge->len] = node.from;
		merge->to[merge->len] = node.to;
		merge->valid[merge->len] = 1;
		merge->len ++;
	}
	if(merge->before_seg < node.before_seg)
	{
		merge->before_seg = node.before_seg;
	}
	if(merge->after_seg < node.after_seg)
	{
		merge->after_seg = node.after_seg;
	}
	if(merge->seg_from > node.from)
	{
		merge->seg_from = node.from;
	}
	if(merge->seg_to < node.to)
	{
		merge->seg_to = node.to;
	}
	for(i = 0 ; i < merge->len ; i ++)
	{
		if(0 == merge->valid[i])
		{
			for(j = i ; j < merge->len - 1 ; j ++)
			{
				merge->from[j] = merge->from[j + 1];
				merge->to[j]= merge->to[j + 1];
				merge->valid[j] = merge->valid[j + 1];
			}
			merge->len --;
		}
	}
	for(i = 0 ; i < merge->len ; i ++)
	{
		j = i - 1;
		while((j >=0) && (merge->from[j] > merge->from[i]))
			j --;
		j ++;
		from = merge->from[i];
		to = merge->to[i];
		for(k = i ; k > j; k --)
		{
			merge->from[k] = merge->from[k -1];
			merge->to[k] = merge->to[k -1];
		}
		merge->from[j] = from;
		merge->to[j] = to;
	}
	return 0;
}

int merge_load_process(struct load_node * node, struct load_index * lindex, int len, struct merge_load_struct * result)
{
	int i, index;
	uint64_t pre_seg_offset;
	for(i = 0 ; i < len ; i ++)
	{
		result->merge[i].len = 0;
		result->merge[i].after_seg = 0;
		result->merge[i].before_seg = 0;
	}
	result->len = 0;
	index = -1;
	pre_seg_offset = 0xFFFFFFFFFFFFFFFF;
	for(i = 0 ; i < len ; i ++)
	{
		if(pre_seg_offset != node[i].seg_offset)
		{
			index ++;
			result->merge[index].seg_offset = node[i].seg_offset;
			result->merge[index].from[0] = node[i].from;
			result->merge[index].to[0] = node[i].to;
			result->merge[index].before_seg = node[i].before_seg;
			result->merge[index].after_seg = node[i].after_seg;
			result->merge[index].len ++;
			result->merge[index].valid[0] = 1;
			result->merge[index].seg_from = node[i].from;
			result->merge[index].seg_to = node[i].to;
			pre_seg_offset = node[i].seg_offset;

		}
		else
		{
			merge_node(node[i], &result->merge[index]);
		}
		lindex[node[i].rindex].merge_index = index;
	}
	result->len = index + 1;
	return 0;
}

uint32_t merge_get_metadata(struct mtdata_seg * mt_seg, struct metadata * mtdata, struct merge_load_arg * merge_load)
{
	int i, j;
	uint32_t cpy_len, old_cpy_len;
	uint32_t max_to;
	int node_index ,old_index;
	struct load_node node;
	struct merge_load_struct result;
	uint64_t pre_seg_offset;
	uint32_t merged_result_len;
	char read_buf_a[SEG_SIZE];
	char read_buf_b[SEG_SIZE];
	char read_buf_c[SEG_SIZE];
	uint64_t read_offset;
	uint64_t read_end_offset;
	uint32_t read_len;
	char * curr_seg;
	char * pre_seg;
	char * next_seg;
	char * tmp_seg;
	struct mtdata_seg_header * header;
	struct timeval start ,end;

	fill_attributes_of_load_node(merge_load->node, merge_load->len);
	rank_load_node(merge_load->node, merge_load->index, merge_load->len);
	merge_load_process(merge_load->node, merge_load->index, merge_load->len, &result);


	node_index = 0;
	merged_result_len = result.len;
	pre_seg = read_buf_a;
	curr_seg = read_buf_b;
	next_seg = read_buf_c;
	pre_seg_offset = 0XFFFFFFFFFFFFFFFE;
	cpy_len = 0;
	for(i = 0 ; i < merged_result_len ; i ++)
	{
		if(result.merge[i].seg_offset == mt_seg->mt_seg_offset)
		{
			memcpy(curr_seg, &mt_seg->header, sizeof(struct mtdata_seg_header));
			memcpy(curr_seg + sizeof(struct mtdata_seg_header), mt_seg->mtdata, mt_seg->len * sizeof(struct metadata));
			max_to = mt_seg->len;
		}
		else
		{
			if((i + 1 < merged_result_len) && (result.merge[i + 1].before_seg > 0))
			{
				if(result.merge[i].seg_from > MTDATA_PER_SEGMENT - result.merge[i +1].before_seg)
				{
					result.merge[i].seg_from = MTDATA_PER_SEGMENT - result.merge[i +1].before_seg;
				}
			}
			if((result.merge[i].before_seg > 0) || (result.merge[i].after_seg > 0))
			{
				read_offset = result.merge[i].seg_offset;
			}
			else
			{
				read_offset = result.merge[i].seg_offset + (result.merge[i].seg_from * sizeof(struct metadata)) + sizeof(struct mtdata_seg_header);
			}
			if((i + 1 < merged_result_len) && result.merge[i+1].before_seg > 0)
			{
				read_end_offset = result.merge[i].seg_offset + SEG_SIZE;
			}
			else
			{
				read_end_offset = result.merge[i].seg_offset + (result.merge[i].seg_to * sizeof(struct metadata)) + sizeof(struct mtdata_seg_header);
			}
			read_len = read_end_offset - read_offset;
			max_to = (read_end_offset - result.merge[i].seg_offset - sizeof(struct mtdata_seg_header)) / sizeof(struct metadata);
			gettimeofday(&start, NULL);
			simpleread(read_offset, curr_seg + read_offset - result.merge[i].seg_offset, read_len, mt_seg->manager);
			gettimeofday(&end, NULL);
			load_cache_read_times ++;
			this_load_cache_read_times ++;
			this_load_cache_read_time += td(&start, &end);
			load_cache_read_time += td(&start, &end);
			load_cache_read_len += read_len;
			this_load_cache_read_len += read_len;

		}

		if(result.merge[i].before_seg > 0)
		{
			header = curr_seg;
			if(header->previous != pre_seg_offset)
			{
				read_offset = header->previous + sizeof(struct mtdata_seg_header) + (MTDATA_PER_SEGMENT - result.merge[i].before_seg) * sizeof(struct metadata);
				read_len = sizeof(struct metadata) * result.merge[i].before_seg;
				gettimeofday(&start, NULL);
				simpleread(read_offset, pre_seg + read_offset - header->previous, read_len, mt_seg->manager);
				gettimeofday(&end, NULL);
				load_cache_read_times ++;
				this_load_cache_read_times ++;
				this_load_cache_read_time += td(&start, &end);
				load_cache_read_time += td(&start, &end);
				load_cache_read_len += read_len;
				this_load_cache_read_len += read_len;

				memcpy(mtdata, pre_seg + sizeof(struct mtdata_seg_header) + (MTDATA_PER_SEGMENT - result.merge[i].before_seg)* sizeof(struct metadata), sizeof(struct metadata) * result.merge[i].before_seg);
				cpy_len += result.merge[i].before_seg;
				mtdata += result.merge[i].before_seg;
			}
			else
			{
				if(result.merge[i - 1].seg_to <= (MTDATA_PER_SEGMENT -result.merge[i].before_seg))
				{
					memcpy(mtdata, pre_seg + sizeof(struct mtdata_seg_header) + (MTDATA_PER_SEGMENT - result.merge[i].before_seg)* sizeof(struct metadata), sizeof(struct metadata) * result.merge[i].before_seg);
					cpy_len += result.merge[i].before_seg;
					mtdata += result.merge[i].before_seg;
				}
				else
				{
					if(result.merge[i- 1].from[result.merge[i - 1].len - 1] > ( MTDATA_PER_SEGMENT - result.merge[i].before_seg))
					{
						node.before = result.merge[i].before_seg - 1;
						node.after = 1;
						node.offset = node.offset = pre_seg_offset + sizeof(struct mtdata_seg_header) + MTDATA_PER_SEGMENT * sizeof(struct metadata) - sizeof(struct metadata);
						fill_attributes_of_load_node(&node, 1);

						node_index --;
						while((node_index >= 0) && (merge_load->node[node_index].seg_offset == pre_seg_offset))
						{
							merge_load->index[merge_load->node[node_index].rindex].len = 0;
							node_index --;
						}
						node_index ++;
						i --;
						for(j = 0 ; j < result.merge[i].len ; j ++)
						{
							cpy_len -= result.merge[i].to[j] - result.merge[i].from[j];
							mtdata -= result.merge[i].to[j] - result.merge[i].from[j];
						}
						//cpy_len -= result.merge[i].before_seg;
						//pre_seg_offset = 0XFFFFFFFFFFFFFFFF;
						result.merge[i].before_seg = 0;
						merge_node(node, &result.merge[i]);
						i --;
						continue;
					}
					else
					{
						memcpy(mtdata, pre_seg + sizeof(struct mtdata_seg_header) + result.merge[i-1].seg_to* sizeof(struct metadata), sizeof(struct metadata) * (MTDATA_PER_SEGMENT - result.merge[i-1].seg_to));
						cpy_len += (MTDATA_PER_SEGMENT - result.merge[i-1].seg_to);
						mtdata += (MTDATA_PER_SEGMENT - result.merge[i-1].seg_to);
					}
				}
			}

			old_index = node_index;
			while((node_index < merge_load->len) && (merge_load->node[node_index].seg_offset == result.merge[i].seg_offset))
			{
				if(merge_load->node[node_index].before_seg > 0)
				{
					merge_load->index[merge_load->node[node_index].rindex].mt_offset = cpy_len - merge_load->node[node_index].before_seg;
					merge_load->index[merge_load->node[node_index].rindex].len += merge_load->node[node_index].before_seg;
				}
				node_index ++;
			}
			node_index = old_index;
		}

		for(j = 0 ; j < result.merge[i].len ; j ++)
		{
			old_cpy_len = cpy_len;
			if(1 == result.merge[i].valid[j])
			{
				if(max_to >= result.merge[i].to[j])
				{
					memcpy(mtdata, curr_seg + sizeof(struct mtdata_seg_header) + result.merge[i].from[j] * sizeof(struct metadata), sizeof(struct metadata) * (result.merge[i].to[j] - result.merge[i].from[j]));
					cpy_len += result.merge[i].to[j] - result.merge[i].from[j];
					mtdata += result.merge[i].to[j] - result.merge[i].from[j];
				}
				else
				{
					memcpy(mtdata, curr_seg + sizeof(struct mtdata_seg_header) + result.merge[i].from[j] * sizeof(struct metadata), sizeof(struct metadata) * (max_to - result.merge[i].from[j]));
					cpy_len += max_to - result.merge[i].from[j];
					mtdata += max_to - result.merge[i].from[j];
				}

				old_index = node_index;
				while((node_index < merge_load->len) && (merge_load->node[node_index].seg_offset == result.merge[i].seg_offset))
				{
					if((merge_load->node[node_index].from >= result.merge[i].from[j]) && (merge_load->node[node_index].to <= result.merge[i].to[j]))
					{

						if(merge_load->node[node_index].before_seg == 0)
						{
							merge_load->index[merge_load->node[node_index].rindex].mt_offset = old_cpy_len + (merge_load->node[node_index].from - result.merge[i].from[j]);
						}
						if(merge_load->node[node_index].to <= max_to)
						{
							merge_load->index[merge_load->node[node_index].rindex].len += merge_load->node[node_index].to - merge_load->node[node_index].from;
						}
						else
						{
							merge_load->index[merge_load->node[node_index].rindex].len += max_to - merge_load->node[node_index].from;
						}
					}
					node_index ++;
				}
				node_index = old_index;

			}
		}
		if(result.merge[i].after_seg > 0)
		{
			header = curr_seg;
			if((i + 1 < merged_result_len) && (result.merge[i + 1].seg_offset == header->next))
			{
				node.before = 0;
				node.offset = header->next + sizeof(struct mtdata_seg_header);
				node.after = result.merge[i].after_seg;
				fill_attributes_of_load_node(&node, 1);
				merge_node(node, &result.merge[i + 1]);
				if(header->next == mt_seg->mt_seg_offset)
				{
					max_to = mt_seg->len;
				}
				else
				{
					max_to = result.merge[i].after_seg;
				}
			}
			else
			{
				if(header->next != mt_seg->mt_seg_offset)
				{
					read_offset = header->next + sizeof(struct mtdata_seg_header);
					read_len = result.merge[i].after_seg * sizeof(struct metadata);
					gettimeofday(&start, NULL);
					simpleread(read_offset, next_seg + sizeof(struct mtdata_seg_header), read_len, mt_seg->manager);
					gettimeofday(&end, NULL);
					load_cache_read_times ++;
					this_load_cache_read_times ++;
					this_load_cache_read_time += td(&start, &end);
					load_cache_read_time += td(&start, &end);
					load_cache_read_len += read_len;
					this_load_cache_read_len += read_len;
					memcpy(mtdata, next_seg + sizeof(struct mtdata_seg_header), sizeof(struct metadata) * result.merge[i].after_seg);
					cpy_len += result.merge[i].after_seg;
					mtdata += result.merge[i].after_seg;
					max_to = result.merge[i].after_seg;
				}
				else
				{
					if(mt_seg->len >= result.merge[i].after_seg)
					{
						memcpy(mtdata, mt_seg->mtdata, sizeof(struct metadata) * result.merge[i].after_seg);
						cpy_len += result.merge[i].after_seg;
						mtdata += result.merge[i].after_seg;
						max_to = result.merge[i].after_seg;
					}
					else
					{
						memcpy(mtdata, mt_seg->mtdata, mt_seg->len * sizeof(struct metadata));
						cpy_len += mt_seg->len;
						mtdata += mt_seg->len;
						max_to = mt_seg->len;
					}
				}
			}
			old_index = node_index;
			while((node_index < merge_load->len) && (merge_load->node[node_index].seg_offset == result.merge[i].seg_offset))
			{
				if(merge_load->node[node_index].after_seg > 0)
				{
					if(max_to >= merge_load->node[node_index].after_seg)
					{
						merge_load->index[merge_load->node[node_index].rindex].len += merge_load->node[node_index].after_seg;
					}
					else
					{
						merge_load->index[merge_load->node[node_index].rindex].len += max_to;
					}
				}
				node_index ++;
			}
			node_index = old_index;
		}

		// loop to make the inode with the same seg_offset to the end
		while((node_index < merge_load->len) && (merge_load->node[node_index].seg_offset == result.merge[i].seg_offset))
		{
			node_index ++;
		}

		tmp_seg = pre_seg;
		pre_seg = curr_seg;
		curr_seg = next_seg;
		next_seg = tmp_seg;
		pre_seg_offset = result.merge[i].seg_offset;
	}
	return cpy_len;
}

int fill_attr_seg_node(struct load_node * node, int len)
{
	int i;
	uint32_t seg_before;
	for(i = 0 ; i < len ; i ++)
	{
		node[i].seg_offset = node[i].offset / SEG_SIZE * SEG_SIZE;
		seg_before = (node[i].offset - node[i].seg_offset - sizeof(struct mtdata_seg_header)) / sizeof(struct metadata);
		if(seg_before >= node[i].before)
		{
			node[i].before_seg = 0;
			node[i].from = seg_before - node[i].before;
		}
		else
		{
			node[i].before_seg = node[i].before - seg_before;
			node[i].from = 0;
		}
		node[i].after_seg = 0;
		node[i].to = MTDATA_PER_SEGMENT;
	}
	return len;
}

uint32_t merge_get_seg_metadata(struct mtdata_seg * mt_seg, struct metadata * mtdata, struct merge_load_arg * merge_load)
{
	int i, j;
	uint32_t cpy_len, old_cpy_len;
	uint32_t max_to;
	int node_index ,old_index;
	struct load_node node;
	struct merge_load_struct result;
	uint64_t pre_seg_offset;
	uint32_t merged_result_len;
	char read_buf_a[SEG_SIZE];
	char read_buf_b[SEG_SIZE];
	char read_buf_c[SEG_SIZE];
	uint64_t read_offset;
	uint64_t read_end_offset;
	uint32_t read_len;
	char * curr_seg;
	char * pre_seg;
	char * next_seg;
	char * tmp_seg;
	struct mtdata_seg_header * header;
	struct timeval start ,end;

	fill_attr_seg_node(merge_load->node, merge_load->len);
	rank_load_node(merge_load->node, merge_load->index, merge_load->len);
	merge_load_process(merge_load->node, merge_load->index, merge_load->len, &result);


	node_index = 0;
	merged_result_len = result.len;
	pre_seg = read_buf_a;
	curr_seg = read_buf_b;
	next_seg = read_buf_c;
	pre_seg_offset = 0XFFFFFFFFFFFFFFFE;
	cpy_len = 0;
	for(i = 0 ; i < merged_result_len ; i ++)
	{
		if(result.merge[i].seg_offset == mt_seg->mt_seg_offset)
		{
			memcpy(curr_seg, &mt_seg->header, sizeof(struct mtdata_seg_header));
			memcpy(curr_seg + sizeof(struct mtdata_seg_header), mt_seg->mtdata, mt_seg->len * sizeof(struct metadata));
			max_to = mt_seg->len;
		}
		else
		{
			if((i + 1 < merged_result_len) && (result.merge[i + 1].before_seg > 0))
			{
				if(result.merge[i].seg_from > MTDATA_PER_SEGMENT - result.merge[i +1].before_seg)
				{
					result.merge[i].seg_from = MTDATA_PER_SEGMENT - result.merge[i +1].before_seg;
				}
			}
			if((result.merge[i].before_seg > 0) || (result.merge[i].after_seg > 0))
			{
				read_offset = result.merge[i].seg_offset;
			}
			else
			{
				read_offset = result.merge[i].seg_offset + (result.merge[i].seg_from * sizeof(struct metadata)) + sizeof(struct mtdata_seg_header);
			}
			if((i + 1 < merged_result_len) && result.merge[i+1].before_seg > 0)
			{
				read_end_offset = result.merge[i].seg_offset + SEG_SIZE;
			}
			else
			{
				read_end_offset = result.merge[i].seg_offset + (result.merge[i].seg_to * sizeof(struct metadata)) + sizeof(struct mtdata_seg_header);
			}
			read_len = read_end_offset - read_offset;
			max_to = (read_end_offset - result.merge[i].seg_offset - sizeof(struct mtdata_seg_header)) / sizeof(struct metadata);
			gettimeofday(&start, NULL);
			simpleread(read_offset, curr_seg + read_offset - result.merge[i].seg_offset, read_len, mt_seg->manager);
			gettimeofday(&end, NULL);
			load_cache_read_times ++;
			this_load_cache_read_times ++;
			this_load_cache_read_time += td(&start, &end);
			load_cache_read_time += td(&start, &end);
			load_cache_read_len += read_len;
			this_load_cache_read_len += read_len;

		}

		if(result.merge[i].before_seg > 0)
		{
			header = curr_seg;
			if(header->previous != pre_seg_offset)
			{
				read_offset = header->previous + sizeof(struct mtdata_seg_header) + (MTDATA_PER_SEGMENT - result.merge[i].before_seg) * sizeof(struct metadata);
				read_len = sizeof(struct metadata) * result.merge[i].before_seg;
				gettimeofday(&start, NULL);
				simpleread(read_offset, pre_seg + read_offset - header->previous, read_len, mt_seg->manager);
				gettimeofday(&end, NULL);
				load_cache_read_times ++;
				this_load_cache_read_times ++;
				this_load_cache_read_time += td(&start, &end);
				load_cache_read_time += td(&start, &end);
				load_cache_read_len += read_len;
				this_load_cache_read_len += read_len;

				memcpy(mtdata, pre_seg + sizeof(struct mtdata_seg_header) + (MTDATA_PER_SEGMENT - result.merge[i].before_seg)* sizeof(struct metadata), sizeof(struct metadata) * result.merge[i].before_seg);
				cpy_len += result.merge[i].before_seg;
				mtdata += result.merge[i].before_seg;
			}
			else
			{
				if(result.merge[i - 1].seg_to <= (MTDATA_PER_SEGMENT -result.merge[i].before_seg))
				{
					memcpy(mtdata, pre_seg + sizeof(struct mtdata_seg_header) + (MTDATA_PER_SEGMENT - result.merge[i].before_seg)* sizeof(struct metadata), sizeof(struct metadata) * result.merge[i].before_seg);
					cpy_len += result.merge[i].before_seg;
					mtdata += result.merge[i].before_seg;
				}
				else
				{
					if(result.merge[i- 1].from[result.merge[i - 1].len - 1] > ( MTDATA_PER_SEGMENT - result.merge[i].before_seg))
					{
						node.before = result.merge[i].before_seg - 1;
						node.after = 1;
						node.offset = pre_seg_offset + sizeof(struct mtdata_seg_header) + MTDATA_PER_SEGMENT * sizeof(struct metadata) - sizeof(struct metadata);
						fill_attributes_of_load_node(&node, 1);

						node_index --;
						while((node_index >= 0) && (merge_load->node[node_index].seg_offset == pre_seg_offset))
						{
							merge_load->index[merge_load->node[node_index].rindex].len = 0;
							node_index --;
						}
						node_index ++;
						i --;
						for(j = 0 ; j < result.merge[i].len ; j ++)
						{
							cpy_len -= result.merge[i].to[j] - result.merge[i].from[j];
							mtdata -= result.merge[i].to[j] - result.merge[i].from[j];
						}
						//cpy_len -= result.merge[i].before_seg;
						//pre_seg_offset = 0XFFFFFFFFFFFFFFFF;
						result.merge[i].before_seg = 0;
						merge_node(node, &result.merge[i]);
						i --;
						continue;
					}
					else
					{
						memcpy(mtdata, pre_seg + sizeof(struct mtdata_seg_header) + result.merge[i-1].seg_to* sizeof(struct metadata), sizeof(struct metadata) * (MTDATA_PER_SEGMENT - result.merge[i-1].seg_to));
						cpy_len += (MTDATA_PER_SEGMENT - result.merge[i-1].seg_to);
						mtdata += (MTDATA_PER_SEGMENT - result.merge[i-1].seg_to);
					}
				}
			}

			old_index = node_index;
			while((node_index < merge_load->len) && (merge_load->node[node_index].seg_offset == result.merge[i].seg_offset))
			{
				if(merge_load->node[node_index].before_seg > 0)
				{
					merge_load->index[merge_load->node[node_index].rindex].mt_offset = cpy_len - merge_load->node[node_index].before_seg;
					merge_load->index[merge_load->node[node_index].rindex].len += merge_load->node[node_index].before_seg;
				}
				node_index ++;
			}
			node_index = old_index;
		}

		for(j = 0 ; j < result.merge[i].len ; j ++)
		{
			old_cpy_len = cpy_len;
			if(1 == result.merge[i].valid[j])
			{
				if(max_to >= result.merge[i].to[j])
				{
					memcpy(mtdata, curr_seg + sizeof(struct mtdata_seg_header) + result.merge[i].from[j] * sizeof(struct metadata), sizeof(struct metadata) * (result.merge[i].to[j] - result.merge[i].from[j]));
					cpy_len += result.merge[i].to[j] - result.merge[i].from[j];
					mtdata += result.merge[i].to[j] - result.merge[i].from[j];
				}
				else
				{
					memcpy(mtdata, curr_seg + sizeof(struct mtdata_seg_header) + result.merge[i].from[j] * sizeof(struct metadata), sizeof(struct metadata) * (max_to - result.merge[i].from[j]));
					cpy_len += max_to - result.merge[i].from[j];
					mtdata += max_to - result.merge[i].from[j];
				}

				old_index = node_index;
				while((node_index < merge_load->len) && (merge_load->node[node_index].seg_offset == result.merge[i].seg_offset))
				{
					if((merge_load->node[node_index].from >= result.merge[i].from[j]) && (merge_load->node[node_index].to <= result.merge[i].to[j]))
					{

						if(merge_load->node[node_index].before_seg == 0)
						{
							merge_load->index[merge_load->node[node_index].rindex].mt_offset = old_cpy_len + (merge_load->node[node_index].from - result.merge[i].from[j]);
						}
						if(merge_load->node[node_index].to <= max_to)
						{
							merge_load->index[merge_load->node[node_index].rindex].len += merge_load->node[node_index].to - merge_load->node[node_index].from;
						}
						else
						{
							merge_load->index[merge_load->node[node_index].rindex].len += max_to - merge_load->node[node_index].from;
						}
					}
					node_index ++;
				}
				node_index = old_index;

			}
		}
		if(result.merge[i].after_seg > 0)
		{
			header = curr_seg;
			if((i + 1 < merged_result_len) && (result.merge[i + 1].seg_offset == header->next))
			{
				node.before = 0;
				node.offset = header->next + sizeof(struct mtdata_seg_header);
				node.after = result.merge[i].after_seg;
				fill_attributes_of_load_node(&node, 1);
				merge_node(node, &result.merge[i + 1]);
				if(header->next == mt_seg->mt_seg_offset)
				{
					max_to = mt_seg->len;
				}
				else
				{
					max_to = result.merge[i].after_seg;
				}
			}
			else
			{
				if(header->next != mt_seg->mt_seg_offset)
				{
					read_offset = header->next + sizeof(struct mtdata_seg_header);
					read_len = result.merge[i].after_seg * sizeof(struct metadata);
					gettimeofday(&start, NULL);
					simpleread(read_offset, next_seg + sizeof(struct mtdata_seg_header), read_len, mt_seg->manager);
					gettimeofday(&end, NULL);
					load_cache_read_times ++;
					this_load_cache_read_times ++;
					this_load_cache_read_time += td(&start, &end);
					load_cache_read_time += td(&start, &end);
					load_cache_read_len += read_len;
					this_load_cache_read_len += read_len;
					memcpy(mtdata, next_seg + sizeof(struct mtdata_seg_header), sizeof(struct metadata) * result.merge[i].after_seg);
					cpy_len += result.merge[i].after_seg;
					mtdata += result.merge[i].after_seg;
					max_to = result.merge[i].after_seg;
				}
				else
				{
					if(mt_seg->len >= result.merge[i].after_seg)
					{
						memcpy(mtdata, mt_seg->mtdata, sizeof(struct metadata) * result.merge[i].after_seg);
						cpy_len += result.merge[i].after_seg;
						mtdata += result.merge[i].after_seg;
						max_to = result.merge[i].after_seg;
					}
					else
					{
						memcpy(mtdata, mt_seg->mtdata, mt_seg->len * sizeof(struct metadata));
						cpy_len += mt_seg->len;
						mtdata += mt_seg->len;
						max_to = mt_seg->len;
					}
				}
			}
			old_index = node_index;
			while((node_index < merge_load->len) && (merge_load->node[node_index].seg_offset == result.merge[i].seg_offset))
			{
				if(merge_load->node[node_index].after_seg > 0)
				{
					if(max_to >= merge_load->node[node_index].after_seg)
					{
						merge_load->index[merge_load->node[node_index].rindex].len += merge_load->node[node_index].after_seg;
					}
					else
					{
						merge_load->index[merge_load->node[node_index].rindex].len += max_to;
					}
				}
				node_index ++;
			}
			node_index = old_index;
		}

		// loop to make the inode with the same seg_offset to the end
		while((node_index < merge_load->len) && (merge_load->node[node_index].seg_offset == result.merge[i].seg_offset))
		{
			node_index ++;
		}

		tmp_seg = pre_seg;
		pre_seg = curr_seg;
		curr_seg = next_seg;
		next_seg = tmp_seg;
		pre_seg_offset = result.merge[i].seg_offset;
	}
	return cpy_len;
}

#endif /* METADATA_H_ */
