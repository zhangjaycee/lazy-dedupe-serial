/*
 * chunk.c
 *
 *  Created on: 2012-7-13
 *      Author: BadBoy
 */
#ifndef CHUNK_H_
#define CHUNK_H_

#include <stdlib.h>
#include <stdint.h>
#include "adler32.h"
#include "rabin-hash.h"
#include "config.h"
#include "dedup.h"

int chunk_haha = 0;

int chunk(char * chunk_buf, char * data, size_t len, int cont, int file_end, struct dedup_manager * de_dup)
{
	static int in_chunk_len = 0;
	struct timeval start, end;
#ifndef FIXED_CHUNK
	static char first;
	static uint32_t abstract;
	static char * win_start;
	char new;
#endif
	uint32_t chunk_num;

	int parsed_len;
	int cpy_len;

	gettimeofday(&start, NULL);

	if(NULL == chunk_buf)
		return -1;
	if(NULL == data)
		return -1;

	parsed_len = 0;
	//if the data is not continued with the previous one, clean the data in chunk_buf
	if(0 == cont)
	{
		in_chunk_len = 0;
	}
	chunk_num = 0;
	while(parsed_len < len)
	{
#ifndef FIXED_CHUNK
		if(in_chunk_len < MIN_CHUNK_LEN)
		{
			cpy_len = MIN_CHUNK_LEN - in_chunk_len;
			if(cpy_len > (len - parsed_len))
			{
				cpy_len = len - parsed_len;
				memcpy(chunk_buf + in_chunk_len, data + parsed_len, cpy_len);
				in_chunk_len += cpy_len;
				parsed_len += cpy_len;
			}
			else
			{
				memcpy(chunk_buf + in_chunk_len, data + parsed_len, cpy_len);
				in_chunk_len += cpy_len;
				parsed_len += cpy_len;

				win_start = chunk_buf + MIN_CHUNK_LEN - WIN_LEN;
				first = * win_start;
#ifdef ADLER
				abstract = adler32_checksum(win_start, WIN_LEN);
#else
				abstract = rabin_hash(win_start, WIN_LEN);
#endif
				if((abstract % MEAN_CHUNK_LEN) == CHUNK_CDC_R)//if((abstract & CHUNK_MASK) == CHUNK_MASK)
				{
					// a chunk
					dedup(chunk_buf, in_chunk_len, de_dup);
					in_chunk_len = 0;
					chunk_num ++;
				}
			}
		}
		else
		{
			new = *(data + parsed_len);
			chunk_buf[in_chunk_len] = data[parsed_len];
			in_chunk_len ++;
			parsed_len ++;

			first = * win_start;
			win_start ++;
#ifdef ADLER
			abstract = adler32_rolling_checksum(abstract, WIN_LEN, first, new);
#else
			abstract = rabin_karp(first, abstract, new);
#endif

			if(((abstract % MEAN_CHUNK_LEN) == CHUNK_CDC_R) || (in_chunk_len >= MAX_CHUNK_LEN))//if((abstract & CHUNK_MASK) == CHUNK_MASK)
			{
				// a chunk
				dedup(chunk_buf, in_chunk_len, de_dup);    //startdedup
				in_chunk_len = 0;
				chunk_num ++;
			}
		}
#else
		cpy_len = MEAN_CHUNK_LEN - in_chunk_len;
		if(cpy_len > (len - parsed_len))
		{
			cpy_len = len - parsed_len;
			memcpy(chunk_buf + in_chunk_len, data + parsed_len, cpy_len);
			in_chunk_len += cpy_len;
			parsed_len += cpy_len;
		}
		else
		{
			memcpy(chunk_buf + in_chunk_len, data + parsed_len, cpy_len);
			in_chunk_len += cpy_len;
			parsed_len += cpy_len;
			dedup(chunk_buf, in_chunk_len, de_dup);  //startdedup
			in_chunk_len = 0;
			chunk_num ++;
		}
#endif
	}

	//file ends the last chunk
	if(0 != file_end)
	{
		if(in_chunk_len > 0)
		{
			dedup(chunk_buf, in_chunk_len, de_dup);
			in_chunk_len = 0;
			chunk_num ++;
		}
	}
	gettimeofday(&end, NULL);
	chunk_time += td(&start, &end);
	this_chunk_time += td(&start, &end);
	return chunk_num;
}

#endif
