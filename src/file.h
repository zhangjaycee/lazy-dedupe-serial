/*
 * file.h
 *
 *  Created on: 2012-7-12
 *      Author: BadBoy
 */

#ifndef FILE_H_
#define FILE_H_

#include <stdint.h>
#include <string.h>
#include "config.h"
#include "storage-manager.h"

struct file_seg_header
{
	uint64_t previous;
	uint64_t next;
	uint32_t file_num;
};

struct file
{
	char name[MAX_NAME_LEN];
	uint64_t metadata_start_offset;
	uint64_t metadata_end_offset;
	uint32_t chunk_num;
};

#define FILE_PER_SEG ((SEG_SIZE - sizeof(struct file_seg_header)) / sizeof(struct file))

struct file_seg
{
	uint64_t seg_offset;
	uint64_t start_seg_offset;
	uint32_t len;
	struct storage_manager * manager;
	struct file_seg_header header;
	struct file file[FILE_PER_SEG];
};

int file_init(struct file_seg * f_seg)
{
	f_seg->len = 0;
	f_seg->seg_offset = get_new_seg(f_seg->manager);
	f_seg->start_seg_offset = f_seg->seg_offset;
	f_seg->header.previous = 0xFFFFFFFFFFFFFFFF;
	return 0;
}

int add_2_file(struct file f, struct file_seg * f_seg)
{
	uint64_t write_offset;
	if(f_seg->len >= FILE_PER_SEG)
	{
		write_offset = f_seg->seg_offset;
		f_seg->seg_offset = get_new_seg(f_seg->manager);
		f_seg->header.next = f_seg->seg_offset;
		simplewrite(write_offset, &(f_seg->header), sizeof(struct file_seg_header), f_seg->manager);
		f_seg->header.previous = write_offset;
		write_offset += sizeof(struct file_seg_header);
		simplewrite(write_offset, f_seg->file, sizeof(struct file) * FILE_PER_SEG, f_seg->manager);
		f_seg->len = 0;
	}
	f_seg->file[f_seg->len] = f;
	f_seg->len ++;
	return 0;
}

int get_files(struct file * f, int len, int pos, struct file_seg * f_seg)
{
	int got;
	int diff;
	uint64_t read_offset;
	uint64_t seg_offset;
	struct file_seg_header header;
	if(NULL == f)
		return -1;
	got = 0;
	if(ALL == pos)
	{
		seg_offset = f_seg->start_seg_offset;
		while(seg_offset != f_seg->seg_offset)
		{
			read_offset = seg_offset + sizeof(struct file_seg_header);
			simpleread(read_offset, f, FILE_PER_SEG * sizeof(struct file), f_seg->manager);
			simpleread(seg_offset, &header, sizeof(struct file_seg_header), f_seg->manager);
			seg_offset = header.next;
			f += FILE_PER_SEG;
			got += FILE_PER_SEG;
		}
		memcpy((void *)f, (const void *)f_seg->file, (size_t)f_seg->len * sizeof(struct file));
		got += f_seg->len;
		return got;
	}
	if(END == pos)
	{
		if(len > f_seg->len)
		{
			memcpy(f, f_seg->file, sizeof(struct file) * f_seg->len);
			len -= f_seg->len;
			f += f_seg->len;
			got += f_seg->len;
		}
		else
		{
			diff = f_seg->len - len;
			memcpy(f, &(f_seg->file[diff]), sizeof(struct file) * len);
			got += len;
			len -= len;
		}
		seg_offset = f_seg->header.previous;
		while(len > 0)
		{
			if(len > FILE_PER_SEG)
			{
				read_offset = seg_offset + sizeof(struct file_seg_header);
				simpleread(read_offset, f, FILE_PER_SEG * sizeof(struct file), f_seg->manager);
				simpleread(seg_offset, &header, sizeof(struct file_seg_header), f_seg->manager);
				seg_offset = header.previous;
				f += FILE_PER_SEG;
				got += FILE_PER_SEG;
				len -= FILE_PER_SEG;
			}
			else
			{
				diff = FILE_PER_SEG - len;
				read_offset = seg_offset + sizeof(struct file_seg_header) + sizeof(struct file) * diff;
				simpleread(read_offset, f, len * sizeof(struct file), f_seg->manager);
				got += len;
				len -= len;
			}
		}
	}
	if(START == pos)
	{
		seg_offset = f_seg->start_seg_offset;
		while(len > 0)
		{
			if(seg_offset != f_seg->seg_offset)
			{
				if(len > FILE_PER_SEG)
				{
					read_offset = seg_offset + sizeof(struct file_seg_header);
					simpleread(read_offset, f, FILE_PER_SEG * sizeof(struct file), f_seg->manager);
					simpleread(seg_offset, &header, sizeof(struct file_seg_header), f_seg->manager);
					seg_offset = header.next;
					f += FILE_PER_SEG;
					got += FILE_PER_SEG;
					len -= FILE_PER_SEG;
				}
				else
				{
					read_offset = seg_offset + sizeof(struct file_seg_header);
					simpleread(read_offset, f, len * sizeof(struct file), f_seg->manager);
					got += len;
					len -= len;
				}
			}
			else
			{
				if(len > f_seg->len)
				{
					memcpy(f, f_seg->file, f_seg->len * sizeof(struct file));
					got += f_seg->len;
					len -= f_seg->len;
					break;
				}
				else
				{
					memcpy(f, f_seg->file, len * sizeof(struct file));
					got += len;
					len -= len;
				}
				break;
			}
		}
	}
	return got;
}

#endif /* FILE_H_ */
