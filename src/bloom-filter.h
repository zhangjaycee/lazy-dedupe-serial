/*
 * bloom-filter.h
 *
 *  Created on: 2012-5-31
 *      Author: badboy
 */

#ifndef BLOOM_FILTER_H_
#define BLOOM_FILTER_H_

#include "config.h"


uint64_t hash_func0(char* finger_prints)
{
	uint64_t position, *p;
	p = (uint64_t *) finger_prints;
	position = (*p) % (BF_LEN * 8);
	return position;
}

uint64_t hash_func1(char* finger_prints)
{
	uint64_t position, *p;
	p = (uint64_t *) (finger_prints + 2);
	position = (*p) % (BF_LEN * 8);
	return position;
}

uint64_t hash_func2(char* finger_prints)
{
	uint64_t position, *p;
	p = (uint64_t *) (finger_prints + 4);
	position = (*p) % (BF_LEN * 8);
	return position;
}

uint64_t hash_func3(char* finger_prints)
{
	uint64_t position, *p;
	p = (uint64_t *) (finger_prints + 6);
	position = (*p) % (BF_LEN * 8);
	return position;
}

uint64_t hash_func4(char* finger_prints)
{
	uint64_t position, *p;
	p = (uint64_t *) (finger_prints + 8);
	position = (*p) % (BF_LEN * 8);
	return position;
}

uint64_t hash_func5(char* finger_prints)
{
	uint64_t position, *p;
	p = (uint64_t *) (finger_prints + 10);
	position = (*p) % (BF_LEN * 8);
	return position;
}


int set_bit(uint64_t nr,int * addr)
{
        int     mask, retval;

        addr += nr >> 5;
        mask = 1 << (nr & 0x1f);
        retval = (mask & *addr) != 0;
        *addr |= mask;
        return retval;
}

 int test_bit(uint64_t nr, int * addr)
{
        int     mask;

        addr += nr >> 5;
        mask = 1 << (nr & 0x1f);
        return ((mask & *addr) != 0);
}
/***********************************************************************
* function name = bloomfilter_lookup
* function = lookup the abstract in the bf and update the bf line
* input :
*         @bfset               the bloom filter bit set
*	    @element          the data abstract from the sha1
*
* return:
*          ret = 1                  find
*          ret = 0 		     not find
***********************************************************************/
int bloom_filter_lookup(void * bf, char * element)
{
	int ret = 1;
	if(test_bit(hash_func0(element),bf) &&
	  	test_bit(hash_func1(element),bf) &&
	  	test_bit(hash_func2(element),bf) &&
	  	test_bit(hash_func3(element),bf) &&
	  	test_bit(hash_func4(element),bf) &&
	  	test_bit(hash_func5(element),bf) ) {
		return ret;
   	}

	ret = 0;
	set_bit(hash_func0(element),bf);
  	set_bit(hash_func1(element),bf);
  	set_bit(hash_func2(element),bf);
  	set_bit(hash_func3(element),bf);
  	set_bit(hash_func4(element),bf);
  	set_bit(hash_func5(element),bf);

	return ret;
}

int bloomfilter_init(char * bf)
{
	memset(bf, 0, BF_LEN);
	return 0;
}


#endif /* BLOOM_FILTER_H_ */
