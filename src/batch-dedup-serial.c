/*
 ============================================================================
 Name        : batch-dedup-serial.c
 Author      : Ma Jingwei
 Version     :
 Copyright   : Your copyright notice
 Description : Hello World in C, Ansi-style
 ============================================================================
 */
#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <dirent.h>
#include<sys/stat.h>

#include "chunk.h"
#ifdef COMPRESS
#include "lzjb.h"
#endif

#define READ_LEN (16 * 1024 * 1024)

#define DEBUGFILE "infofile.txt"

int filedebug(char *str)
{
	FILE *poutfile;
	if((poutfile = fopen(DEBUGFILE,"a+")) == NULL){
		printf("file open failed!\n");
		return -1;
		}
	fprintf(poutfile,str);
	fclose(poutfile);
	return 0;
}

struct file_head
{
	char name[MAX_NAME_LEN];
	int mtype;
	int msize;
};

int create_file(char * name, struct dedup_manager * dedup)
{
	struct file f;
	strcpy(f.name, name);
	f.metadata_start_offset = dedup->mt_seg.mt_seg_offset + sizeof(struct metadata) * dedup->mt_seg.len + sizeof(struct mtdata_seg_header);
	add_2_file(f, &(dedup->f_seg));
	return 0;
}

int end_file(struct dedup_manager * dedup)
{
	int pos;
	struct file_seg * f_seg;
	f_seg = &(dedup->f_seg);
	pos = f_seg->len -1;
	f_seg->file[pos].metadata_end_offset = dedup->mt_seg.mt_seg_offset + sizeof(struct metadata) * dedup->mt_seg.len + sizeof(struct mtdata_seg_header);
	return 0;
}

int write_file(struct file f, struct dedup_manager * dedup, char * name)
{
	FILE * fp;
	char * buf;
#ifdef COMPRESS
	char * uncom_buf;
	int uncom_len;
#endif
	int ret;
	int i;
	fp = fopen(name, "w+");
	struct metadata * mtdata;
	mtdata = malloc(f.chunk_num * (sizeof(struct metadata)));
	buf = malloc(MAX_CHUNK_LEN);
#ifdef COMPRESS
	uncom_buf = malloc (MAX_COMPRESS_LEN);
#endif
	ret = get_metadata(&(dedup->mt_seg), mtdata, f.metadata_start_offset, 0, f.chunk_num);
	if(ret < f.chunk_num)
	{
		puts("can't get all metadata for the file");
	}
	for(i = 0 ; i < ret ; i ++)
	{
		get_data(buf, mtdata[i].offset, mtdata[i].len, &(dedup->dt_seg));
#ifdef COMPRESS
		uncom_len = lzjb_decompress(buf, uncom_buf, mtdata[i].len, MAX_COMPRESS_LEN, 0);
		fwrite(uncom_buf, 1, mtdata[i].origin_len, fp);
#else
		fwrite(buf, 1, mtdata[i].len, fp);
#endif
	}
	fclose(fp);
	free(mtdata);
	free(buf);
#ifdef COMPRESS
	free(uncom_buf);
#endif
	return 0;
}


int backup_file(char * name, struct dedup_manager * dedup)
{
	struct file_head head;
	struct stat s;
	struct timeval start, end;
	uint64_t read_len = 0;
	FILE * fp;
	char read_buf[READ_BUF_SIZE];
	int ret;
	bzero(&head, sizeof(struct file_head));
	strncpy(head.name, name, MAX_NAME_LEN);
	gettimeofday(&start, NULL);
	if(lstat(name, &s) < 0)
	{
		printf("%s does not exist\r\n", name);
	}
	gettimeofday(&end, NULL);
	read_file_time += td(&start, &end);
	this_read_file_time += td(&start, &end);
	head.mtype = REG_FILE;
	head.msize = s.st_size;
//	chunk(dedup->chunk_buf, &head, sizeof(struct file_head), 1, 0, dedup);//to_chunk_buf(&p_d->chunk, (char *)&head, sizeof(struct file_head));
	gettimeofday(&start, NULL);
	if((fp = fopen(name,"r"))  == NULL )
	{
		printf(stderr,"Fail to open the file: %s\n", name);
		return 0;
	}
	gettimeofday(&end, NULL);
	read_file_time += td(&start, &end);
	this_read_file_time += td(&start, &end);
	read_len = 0;
	while(read_len < s.st_size)
	{
		gettimeofday(&start, NULL);
		ret = fread(read_buf, 1, READ_BUF_SIZE, fp);
		read_len += ret;
		gettimeofday(&end, NULL);
		read_file_time += td(&start, &end);
		this_read_file_time += td(&start, &end);
		chunk(dedup->chunk_buf, read_buf, ret, 1, 0, dedup);
		if(data_len >= (1024*1024*1024))
		{
			fclose(fp);
			return 0;
		}
	}
	fclose(fp);
	return 0;
}

int backup_dir(char * name, struct dedup_manager * dedup)
{
	DIR	*source = NULL;
	struct dirent *ent = NULL;
	char child_name[MAX_NAME_LEN];
	struct file_head head;
	struct timeval start, end;
	bzero(&head, sizeof(struct file_head));
	strncpy(head.name, name, MAX_NAME_LEN);
	head.mtype = DIR_FILE;
	chunk(dedup->chunk_buf, &head, sizeof(struct file_head), 1, 0, dedup);//to_chunk_buf(&p_d->chunk, (char *)&head, sizeof(struct file_head));
	gettimeofday(&start, NULL);
	source = opendir(name);
	if(source == NULL)
	{
		perror("Fail to opendir\n");
		return 0;
	}
	gettimeofday(&end, NULL);
	read_file_time += td(&start, &end);
	this_read_file_time += td(&start, &end);
	gettimeofday(&start, NULL);
	while((ent = readdir(source)) != NULL)
	{
		gettimeofday(&end, NULL);
		read_file_time += td(&start, &end);
		this_read_file_time += td(&start, &end);
		if(strcmp(ent->d_name,"..")  != 0 && strcmp(ent->d_name, ".") != 0)
		{
			strcpy(child_name,"\0");
			strcat(child_name, name);
			strcat(child_name,"/");
			strcat(child_name,ent->d_name);
			if(ent->d_type == 4)
				backup_dir(child_name, dedup);
			else if(ent->d_type == 8)
				backup_file(child_name, dedup);
		}
		gettimeofday(&start, NULL);
	}
	closedir(source);
	return 0;
}

int backup(char* name, struct dedup_manager * dedup)
{
	static uint64_t mt_counter = 0;
	struct file backup_root;
	struct stat s;

	if(lstat(name, &s) < 0)
	{
		printf("lstat error for %s\r\n", name);
		return -1;
	}

	backup_root.chunk_num = 0;
//	create_file(&p_d->manager, mt_counter);
	strncpy(backup_root.name, name, MAX_NAME_LEN);

	if(S_ISDIR(s.st_mode))
	{
		printf("%s is a directory\r\n", name);
		backup_dir(name, dedup);
	}
	else
	{
		if(S_ISREG(s.st_mode))
		{
			printf("%s is a regular file\r\n", name);
			backup_file(name, dedup);
		}
	}

//	ret = chunk(BUF, 0, 1, 1, &p_d->chunk);
//	mt_counter += ret;
//	backup_file.chunk_num += ret;
//	end_file(&p_d->manager, backup_root);

	return 0;
}


int main(int argc, char* argv[])
{
	struct dedup_manager * dedup;
	char info[200];
	struct timeval start, end;
	argc--;
	argv++; //skip the program name itself
	int i;

#ifdef DEBUG
	compress_time = 0;
	hash_time = 0;
	identify_time = 0;
	encrypt_time = 0;
	store_time = 0;
	data_len = 0;
	chunk_time = 0;
	compress_len = 0;
	unique_len = 0;
	dup_len = 0;

	printf("size of metadtat %d\n", sizeof(struct metadata));
	printf("size of struct dedup_manager %ldMB\n", sizeof(struct dedup_manager) / 1024 /1024);
	printf("size of struct storage_manager %ld\n", sizeof(struct storage_manager));
	printf("size of struct mtdata_seg %ldMB\n", sizeof(struct mtdata_seg)/1024/1024);
	printf("size of struct data_seg %ldMB\n", sizeof(struct data_seg)/1024/1024);
	printf("size of struct file_seg %ldMB\n", sizeof(struct file_seg)/1024/1024);
	printf("size of struct cache %ldMB\n", sizeof(struct cache)/1024/1024);
	printf("size of struct disk_hash %ldMB\n", sizeof(struct disk_hash)/1024/1024);


	/*sprintf(info, "size of struct storage_manager %d\n", sizeof(struct storage_manager));
	filedebug(info);
	sprintf(info, "size of struct mtdata_seg %dMB\n", sizeof(struct mtdata_seg)/1024/1024);
	filedebug(info);
	sprintf(info, "size of struct data_seg %dMB\n", sizeof(struct data_seg)/1024/1024);
	filedebug(info);
	sprintf(info, "size of struct file_seg %dMB\n", sizeof(struct file_seg)/1024/1024);
	filedebug(info);
	sprintf(info, "size of struct cache %dMB\n", sizeof(struct cache)/1024/1024);
	filedebug(info);
	sprintf(info, "size of struct disk_hash %dMB\n", sizeof(struct disk_hash)/1024/1024);
	filedebug(info);
	//printf("size of struct chunk_node %d\n", sizeof(struct chunk_node));*/
#endif

	bzero(cache_count, sizeof(int) * 1024);

	dedup = malloc(sizeof(struct dedup_manager));
	if(NULL == dedup)
	{
		puts("can't malloc so big memblock");
		return -1;
	}
	dedup_init(dedup, *argv);
	argv ++;argc --; //skip the metadatafile

#ifdef WRITE_FINGER_TO_FILE
	w_f = fopen("fingerprint", "wb");
	if(NULL == w_f)
	{
		puts("can not open file");
		exit(0);
	}
#endif

#ifdef FINGER_DEBUG
	FILE * finger_file;
	char readfinger[FINGERPRINT_LEN];
	finger_file = fopen(*argv, "rb");
	while(fread(readfinger, 1, FINGERPRINT_LEN, finger_file) == FINGERPRINT_LEN)
	{
		debug_dedup(dedup, readfinger);
	}
	fclose(finger_file);
#else

	gettimeofday(&previous_time, NULL);
	gettimeofday(&start, NULL);
	while(argc > 0)
	{
		backup(*argv, dedup);
		argv ++; argc --;
	}
	gettimeofday(&end, NULL);
	backup_time += td(&start, &end);
#endif

#ifdef WRITE_FINGER_TO_FILE
	fclose(w_f);
#endif

	identify_time -= store_time;
	printf("data len %f MB\n", data_len / (1024 * 1024 * 1.0));
	printf("chunk num %ld, chunk len %ld\n", chunk_num, data_len / chunk_num);
	printf("compress len %f MB\n", compress_len / (1024 * 1024 * 1.0));
	printf("unique len %f MB\n", unique_len / (1024 * 1024 * 1.0));
	printf("dup len %f MB\n", dup_len / (1024 * 1024 * 1.0));
	printf("disk hash lookup time %f\n", disk_hash_look_time / (1024 * 1024 * 1.0));
	printf("defrag time %f\n", defrag_time / (1024 * 1024 * 1.0));
	printf("load cache time %f\n", load_cache_time / 1000000.0);
	printf("load cache times %d\n", load_cache_times);
	printf("load cache read time %f\n", load_cache_read_time / 1000000.0);
	printf("load cache fread times %ld\n", load_cache_read_times);
	printf("load cache fread len %f MB\n", load_cache_read_len / (1024.0 * 1024));
	printf("mem hash cache lookup time %f\n", mem_hash_cache_look_time / (1024 * 1024 * 1.0));
	printf("backup time %f\n", backup_time / 1000000.0);
	printf("backup throughtput is %fMB/s\n", (data_len / (1024 * 1024.0)) / (backup_time / (1000000.0)));
	printf("compress time %f\n", compress_time / 1000000.0);
	printf("hash time %f\n", hash_time / 1000000.0);
	printf("identify time %f\n", identify_time / 1000000.0);
	printf("encrypt time %f\n", encrypt_time / 1000000.0);
	printf("store time %f\n", store_time / 1000000.0);
	printf("chunk time %f\n", chunk_time / 1000000.0);
	printf("cache time is %f\n", cache_time / 1000000.0);
	printf("cache times is %d\n", cache_times);
	printf("disk_hash_time is %f\n", disk_hash_look_time / 1000000.0);
	printf("disk_hash_time hit time is %d\n", disk_hash_hit);
	printf("disk hash fread times %d\n", disk_hash_fread_times);
	printf("disk hash fread time %fsec\n", disk_hash_fread_time / (1000 *1000.0));
	printf("disk hash fread len %fMB\n", disk_hash_fread_len / (1024 * 1024.0));
	printf("rank time %f\n", rank_time / (1000 * 1000.0));
	printf("rank read time %f\n", rank_fread_time / (1000 * 1000.0));
	printf("rank write time %f\n", rank_fwrite_time / (1000 * 1000.0));
	printf("defrag time %f\n", defrag_time / (1024 * 1024 * 1.0));
	printf("dup_num is %d\n", dup_num);
	printf("first cache hit time is %d\n", first_cache_hit);
	printf("second cache hit time is %d\n", second_cache_hit);
	printf("cache hit ratio is %f\n", (first_cache_hit + second_cache_hit) * 1.0/ dup_num);
	printf("bloom filter time is %f\n", bf_time/1000000.0);

	/*sprintf(info, "compress time %f\n", compress_time / 1000000.0);
	filedebug(info);
	sprintf(info, "hash time %f\n", hash_time / 1000000.0);
	filedebug(info);
	sprintf(info, "identify time %f\n", identify_time / 1000000.0);
	filedebug(info);
	sprintf(info, "encrypt time %f\n", encrypt_time / 1000000.0);
	filedebug(info);
	sprintf(info, "store time %f\n", store_time / 1000000.0);
	filedebug(info);
	sprintf(info, "chunk time %f\n", chunk_time / 1000000.0);
	filedebug(info);
	sprintf(info, "disk hash lookup time %f\n", disk_hash_look_time / 1000000.0);
	filedebug(info);
	sprintf(info, "defrag time %f\n", defrag_time / 1000000.0);
	filedebug(info);
	sprintf(info, "load cache time %f\n", load_cache_time);
	filedebug(info);
	sprintf(info, "mem hash cache lookup time %f\n", mem_hash_cache_look_time / 1000000.0);
	filedebug(info);
	sprintf(info, "\n");
	filedebug(info);
	sprintf(info, "data len %f MB\n", data_len / (1024 * 1024 * 1.0));
	filedebug(info);
	sprintf(info, "compress len %f MB\n", compress_len / (1024 * 1024 * 1.0));
	filedebug(info);
	sprintf(info, "unique len %f MB\n", unique_len / (1024 * 1024 * 1.0));
	filedebug(info);
	sprintf(info, "dup len %f MB\n", dup_len / (1024 * 1024 * 1.0));
	filedebug(info);

	sprintf(info, "cache time is %f\n", cache_time / 1000000.0);
	filedebug(info);
	sprintf(info, "disk_hash_time is %f\n", disk_hash_look_time / 1000000.0);
	filedebug(info);
	sprintf(info, "load_cache_time is %f\n", load_cache_time / 1000000.0);
	filedebug(info);
	sprintf(info, "dup_num is %d\n", dup_num);
	filedebug(info);
	sprintf(info, "first cache hit time is %d\n", first_cache_hit);
	filedebug(info);
	sprintf(info, "second cache hit time is %d\n", second_cache_hit);
	filedebug(info);
	sprintf(info, "cache hit ratio is %f\n", (first_cache_hit + second_cache_hit) * 1.0/dup_num);
	filedebug(info);
	sprintf(info, "bloom filter time is %f\n", bf_time/1000000.0);
	filedebug(info);
	sprintf(info, "backup time %f\n", backup_time / 1000000.0);
	filedebug(info);
	sprintf(info, "backup throughtput is %fMB/s\n", (data_len / (1024 * 1024.0)) / (backup_time / (1000000.0)));
	filedebug(info);*/

	/*for(i = 0 ; i < 1024 ; i ++)
		printf("%d,", cache_count[i]);*/
	return EXIT_SUCCESS;
}


