/*
 * data.h
 *
 *  Created on: 2012-7-13
 *      Author: BadBoy
 */

#ifndef DATA_H_
#define DATA_H_

#include <stdint.h>
#include "config.h"

struct data_seg_header
{
	uint64_t previous;
	uint64_t next;
};

#define DATA_PER_SEG (SEG_SIZE - sizeof(struct data_seg_header))

struct data_seg
{
	struct data_seg_header header;
	uint64_t data_seg_offset;
	uint32_t len;
	struct storage_manager * manager;
	char data[DATA_PER_SEG];
};

int data_init(struct data_seg * dt_seg)
{
	dt_seg->data_seg_offset = get_new_seg(dt_seg->manager);
	dt_seg->len = 0;
	return 0;
}

uint64_t add_data(char *buf, uint32_t len, struct data_seg * dt_seg)
{
	uint64_t offset;
	uint64_t write_offset;
	uint32_t left_len;
	uint32_t left_seg_len;
	char * data_p;
	left_len = len;
	data_p = buf;
	offset = dt_seg->data_seg_offset + dt_seg->len + sizeof(struct data_seg_header);
	while(left_len > 0)
	{
		left_seg_len = DATA_PER_SEG - dt_seg->len;
		if(left_seg_len > left_len)
		{
			memcpy(dt_seg->data + dt_seg->len, data_p, left_len);
			dt_seg->len += left_len;
			left_len -= left_len;
			data_p += left_len;
		}
		else
		{
			memcpy(dt_seg->data + dt_seg->len, data_p, left_seg_len);
			dt_seg->len += left_seg_len;
			write_offset = dt_seg->data_seg_offset;
			//dt_seg->data_seg_offset = get_new_seg(dt_seg->manager);
			dt_seg->header.next = dt_seg->data_seg_offset;
			//simplewrite(write_offset, &dt_seg->header, sizeof(struct data_seg_header), dt_seg->manager);
			write_offset += sizeof(struct data_seg_header);
			//simplewrite(write_offset, dt_seg->data, dt_seg->len, dt_seg->manager);
			dt_seg->len = 0;
			left_len -= left_seg_len;
			data_p += left_seg_len;
		}
	}
	return offset;
}

int get_data(char *buf, uint64_t offset, uint32_t len, struct data_seg * dt_seg)
{
	uint32_t diff;
	uint32_t left_seg;
	uint64_t seg_offset;
	struct data_seg_header header;
	while(len > 0)
	{
		if(offset >= dt_seg->data_seg_offset)
		{
			diff = offset - dt_seg->data_seg_offset - sizeof(struct data_seg_header);
			if(len + diff <= dt_seg->len)
			{
				memcpy(buf, dt_seg->data + diff, len);
				len -= len;
				buf += len;
			}
			else
			{
				return -1;
			}
		}
		else
		{
			seg_offset = offset / SEG_SIZE * SEG_SIZE;
			left_seg = seg_offset + SEG_SIZE - offset;
			if(left_seg >= len)
			{
				simpleread(offset, buf, len, dt_seg->manager);
				buf += len;
				len -= len;
			}
			else
			{
				simpleread(offset, buf, left_seg, dt_seg->manager);
				buf += left_seg;
				len -= left_seg;
				simpleread(seg_offset, &header, sizeof(struct data_seg_header), dt_seg->manager);
				offset = header.next + sizeof(struct data_seg_header);
			}
		}
	}
	return 0;
}

#endif /* DATA_H_ */
