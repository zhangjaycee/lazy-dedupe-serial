/*
 * storage-manager.h
 *
 *  Created on: 2012-7-14
 *      Author: BadBoy
 */

#ifndef STORAGE_MANAGER_H_
#define STORAGE_MANAGER_H_

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/file.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "config.h"
#include "my-time.h"


struct storage_manager
{
#ifdef DIRECT_IO
	int f;
	char * rw_buf;
	uint pg_size;
#else
	FILE *f;
#endif
	uint64_t allocted_offset;
};

uint64_t get_new_block(struct storage_manager * manager, uint64_t len)
{
	uint64_t offset;
	offset = manager->allocted_offset;
	manager->allocted_offset += len;
	return offset;
}

uint64_t get_new_seg(struct storage_manager * manager)
{
	return get_new_block(manager, SEG_SIZE);
}

int storage_manager_init(struct storage_manager * manager, char * file_name)
{
#ifdef DIRECT_IO
	manager->f= open(file_name, O_RDWR | O_CREAT | O_TRUNC | O_DIRECT, S_IREAD | S_IWRITE);
	if(manager->f < 0)
	{
		printf("can't open metadatafile %s\n", file_name);
		return -1;
	}
	manager->pg_size =  getpagesize();
	manager->rw_buf = valloc(SEG_SIZE);
	if(NULL == manager->rw_buf)
	{
		printf("can't valloc memory %s\n", file_name);
		return -1;
	}
#else
	manager->f= fopen(file_name, "wb+");
	if(NULL == manager->f)
	{
		printf("can't open metadatafile %s\n", file_name);
		return -1;
	}
#endif
	manager->allocted_offset = 0;
	return 0;
}

int storage_manager_destroy(struct storage_manager * manager)
{
#ifdef DIRECT_IO
	free(manager->rw_buf);
	close(manager->f);
#else
	fclose(manager->f);
#endif
	return 0;
}

void myseek(struct storage_manager * manager, uint64_t a)
{
#ifdef DIRECT_IO
	if (lseek(manager->f, a, SEEK_SET) != a)
#else
	if (fseek(manager->f, a, SEEK_SET) < 0)
#endif
	{
		printf("seek locate %ld\n", a);
		perror("Can not seek locally!");
	}
}

int simpleread(uint64_t a, void *buf, size_t len, struct storage_manager * manager)
{
	size_t res;
#ifdef DIRECT_IO
	int ret;
	uint64_t new_off;
	size_t new_len;
	uint64_t new_end;
#endif
	struct timeval start, end;
	//printf("read offset %ld\n", a);
	gettimeofday(&start, NULL);
#ifdef DIRECT_IO
	new_off = a - (a % manager->pg_size);
	new_end = a +len;
	new_end = (0 == new_end % manager->pg_size) ? new_end : new_end /manager->pg_size * manager->pg_size + manager->pg_size;
	new_len = new_end - new_off;
	myseek(manager, new_off);
	ret = read(manager->f, manager->rw_buf, new_len);
	if(ret < 0)
		return ret;
	memcpy(buf, manager->rw_buf + (a - new_off), len);
#else
	myseek(manager, a);
	res = fread(buf, 1, len, manager->f);
#endif
	gettimeofday(&end, NULL);
	fread_time += td(&start, &end);
	this_fread_time += td(&start, &end);
	fread_times ++;
	this_fread_times ++;
	fread_len += len;
	this_fread_len += len;
	return (res < 0 || (size_t)res != len);
}

int simplewrite(uint64_t a, void *buf, size_t len, struct storage_manager * manager)
{
	size_t res;
#ifdef DIRECT_IO
	int ret;
	uint64_t new_off;
	uint64_t new_end;
	size_t new_len;
#endif
	struct timeval start, end;
	gettimeofday(&start, NULL);
	//printf("write offset %ld\n", a);
#ifdef DIRECT_IO
	new_off = a - (a % manager->pg_size);
	new_end = a +len;
	new_end = (0 == new_end % manager->pg_size) ? new_end : new_end /manager->pg_size * manager->pg_size + manager->pg_size;
	new_len = new_end - new_off;
	if((new_off != a) || (new_len != len))
	{
		myseek(manager, new_off);
		ret = read(manager->f, manager->rw_buf, new_len);
		if(ret < 0)
		{
			printf("can't read");
			exit(0);
			return ret;
		}
	}
	memcpy(manager->rw_buf + (a - new_off), buf, len);
	myseek(manager, new_off);
	ret = write(manager->f, manager->rw_buf, new_len);
	if(ret < 0)
		return ret;
#else
	myseek(manager, a);
	res = fwrite(buf, 1, len, manager->f);
#endif
	gettimeofday(&end, NULL);
	fwrite_time += td(&start, &end);
	this_fwrite_time += td(&start, &end);
	fwrite_times ++;
	this_fwrite_times ++;
	fwrite_len += len;
	this_fwrite_len += len;
	return (res < 0 ||(size_t)res != len);
}

/*int simpleread(uint64_t a, void *buf, size_t len, FILE * handle)
{
	size_t res;
	//printf("read offset %ld\n", a);
	mylock(&wr_lock);
	myseek(handle, a);
	res = fread(buf, 1, len, handle);
	myunlock(&wr_lock);
	return (res < 0 || (size_t)res != len);
}

int simplewrite(uint64_t a, void *buf, size_t len, FILE * handle)
{
	size_t res;
	//printf("write offset %ld\n", a);
	mylock(&wr_lock);
	myseek(handle, a);
	res = fwrite(buf, 1, len, handle);
	myunlock(&wr_lock);
	return (res < 0 ||(size_t)res != len);
}*/

#endif /* STORAGE_MANAGER_H_ */
