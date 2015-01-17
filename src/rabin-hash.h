/*
 * rabin-hash.h
 *
 *  Created on: Aug 7, 2013
 *      Author: badboy
 */

#ifndef RABIN_HASH_H_
#define RABIN_HASH_H_

const unsigned PRIME_BASE = 257;
const unsigned PRIME_MOD = 1000000007;

long long power = 1;

int init_power(int win_len)
{
	int i;
	power = 1;
	for(i = 0 ; i < win_len ; i ++)
		power = (power * PRIME_BASE) % PRIME_MOD;
	return 0;
}

unsigned rabin_hash(const char* data, int len)
{
    long long ret = 0;
    int i;
    for (i = 0; i < len; i++)
    {
    	ret = ret*PRIME_BASE + data[i];
    	ret %= PRIME_MOD; //don't overflow
    }
    return ret;
}

unsigned rabin_karp(const char first, long long pre_checksum, const char last)
{
	pre_checksum = pre_checksum * PRIME_BASE + last;
	pre_checksum %= PRIME_MOD;
	pre_checksum -= power * first % PRIME_MOD;
	if(pre_checksum < 0)
		pre_checksum += PRIME_MOD;
	return pre_checksum;
}


#endif /* RABIN_HASH_H_ */
