/*
 * config.h
 *
 *  Created on: 2012-7-12
 *      Author: BadBoy
 */

#ifndef CONFIG_H_
#define CONFIG_H_

#define DIRECT_IO

#define SEG_SIZE (1024 * 1024)

#define FINGERPRINT_LEN 20

#define CACHE_MASK 0x7ff
#define CACHE_LEN 2048
#define CACHE_BUCKET_LEN 256
//#define CACHE_BUCKET_LEN 10

#define MEM_HASH_NUM 128
#define BIG_MEM_HASH_NUM (MEM_HASH_NUM * 10)
#define BUCKET_NUM 1024
#define DISK_HASH_MASK 0x3ff

//#define CHUNK_MASK 0x01FFF
#define MAX_CHUNK_LEN (16 * 1024)
#define MIN_CHUNK_LEN 512
#define WIN_LEN 48
#define CHUNK_CDC_R 13
//#define ADLER

#define MEAN_CHUNK_LEN 4096

const uint64_t BF_LEN = (1024 * 1024 * 1024);//#define BF_LEN (1024 * 1024 * 1024)

//#define TO_CACHE_LEN 1000

//#define FIXED_CHUNK

#define DEBUG

#define END 0
#define START 1
#define ALL 2

#define COMPRESS

#ifdef COMPRESS
#define MAX_COMPRESS_LEN (2 * MAX_CHUNK_LEN)
#endif

#define FH_LEN 32

#define MAX_NAME_LEN 1000

#define REG_FILE 0
#define DIR_FILE 1

#define READ_BUF_SIZE (1024 * 1024)

#define STATISTIC_LEN (1024 * 1024 * 100)

//#define BATCH_DEDUP

#define LINE_TYPE_CHIE 0
#define LINE_TYPE_HICE 1

#define CUR_LINE_TYPE 3

#define PRE_UNIQUE_THREHOLD 200
#define MAX_BEFORE_LEN 2048
#define LOAD_ADVANCE 100

#define FINGER_DEBUG
//#define WRITE_FINGER_TO_FILE

//#define MERGE_LOAD_CACHE
#define LINE_LOOKUP
#define SEPERATE_LOAD_CACHE

#define DISK_HASH_RANK_LOOKUP

#define CACHE_THRESHOLD 4
#define CACHE_MISS_THRESHOLD 10

#endif /* CONFIG_H_ */
