/*
 * Pure Solo Gateway
 * Coinbase generation for local solo mining
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <stdbool.h>
#include <inttypes.h>
#include <unistd.h>

#include "datum_conf.h"
#include "datum_utils.h"
#include "datum_stratum.h"
#include "datum_coinbaser.h"

const char *cbstart_hex = "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff"; // 82 len hex, 41 bytes

#define MAX_COINBASE_TAG_SPACE 86 // leaves space for BIP34 height, extranonces, tags, etc.

int generate_coinbase_input(int height, char *cb, int *target_pot_index) {
	int cb_input_sz = 0;
	int tag_len[2] = { 0, 0 };
	int k, m, i;
	int excess;
	
	// let's figure out our coinbase tags w/BIP34 height
	i = append_UNum_hex(height, &cb[0]);
	cb_input_sz += i>>1;
	
	// Handle coinbase tagging purely locally for Solo
	tag_len[0] = strlen(datum_config.mining_coinbase_tag_primary);
	tag_len[1] = strlen(datum_config.mining_coinbase_tag_secondary);
	
	k = tag_len[0] + tag_len[1] + 2;
	if (!tag_len[1]) {
		k--;
		if (!tag_len[0]) {
			k--;
		}
	}
	
	if (k > MAX_COINBASE_TAG_SPACE) {
		excess = k - MAX_COINBASE_TAG_SPACE;
		if (tag_len[1] > excess) {
			tag_len[1] -= excess;
			k = MAX_COINBASE_TAG_SPACE;
		} else {
			if (tag_len[1]) {
				tag_len[1] = 0;
				k-=tag_len[1]+1;
			}
		}
	}
	
	if (k > MAX_COINBASE_TAG_SPACE) {
		DLOG_FATAL("Could not fit coinbase primary tag alone somehow. This is probably a bug. Panicking. :(");
		panic_from_thread(__LINE__);
		sleep(1000000);
	}
	
	if (k > 0) {
		if (k <= 75) {
			uchar_to_hex(&cb[i], (unsigned char)k); i+=2; cb_input_sz++;
		} else {
			uchar_to_hex(&cb[i], 0x4C); i+=2; cb_input_sz++;
			uchar_to_hex(&cb[i], (unsigned char)k); i+=2; cb_input_sz++;
		}
		
		if (tag_len[0]) {
			for(m=0;m<tag_len[0];m++) {
				uchar_to_hex(&cb[i], (unsigned char)datum_config.mining_coinbase_tag_primary[m]); i+=2; cb_input_sz++;
			}
			if (!tag_len[1]) {
				uchar_to_hex(&cb[i], 0x00); i+=2; cb_input_sz++;
			} else {
				uchar_to_hex(&cb[i], 0x0F); i+=2; cb_input_sz++;
			}
		} else {
			if (tag_len[1]) {
				uchar_to_hex(&cb[i], 0x0F); i+=2; cb_input_sz++;
			}
		}
		
		if (tag_len[1]) {
			for(m=0;m<tag_len[1];m++) {
				uchar_to_hex(&cb[i], (unsigned char)datum_config.mining_coinbase_tag_secondary[m]); i+=2; cb_input_sz++;
			}
			uchar_to_hex(&cb[i], 0x00); i+=2; cb_input_sz++;
		}
	} else {
		uchar_to_hex(&cb[i], 0x01); i+=2; cb_input_sz++;
		uchar_to_hex(&cb[i], 0x00); i+=2; cb_input_sz++;
	}
	
	uchar_to_hex(&cb[i], 0x03); i+=2; cb_input_sz++;
	if (target_pot_index != NULL) *target_pot_index = cb_input_sz;
	uchar_to_hex(&cb[i], 0xFF); i+=2; cb_input_sz++; 
	uchar_to_hex(&cb[i], (datum_config.coinbase_unique_id&0xFF)); i+=2; cb_input_sz++;
	uchar_to_hex(&cb[i], ((datum_config.coinbase_unique_id>>8)&0xFF)); i+=2; cb_input_sz++;
	
	return cb_input_sz;
}
void generate_base_coinbase_txns_for_stratum_job(T_DATUM_STRATUM_JOB *s) {
	char cb[512];
	int cb_input_sz = 0;
	bool space_for_en_in_coinbase = false;
	int i, j;
	int cb1idx[1] = { 0 };
	int cb2idx[1] = { 0 };
	int target_pot_index;
	
	// Pure Solo: Force resolve the local pool address
	s->pool_addr_script_len = addr_2_output_script(datum_config.mining_pool_address, &s->pool_addr_script[0], 64);
	if (!s->pool_addr_script_len) {
		DLOG_FATAL("Could not generate output script for pool addr! Perhaps invalid? This is bad.");
		panic_from_thread(__LINE__);
	}

	j = strlen(cbstart_hex);
	memcpy(&s->coinbase.coinb1[0], cbstart_hex, j);
	cb1idx[0] = j;
	
	cb_input_sz = generate_coinbase_input(s->height, &cb[0], &target_pot_index);
	i = cb_input_sz << 1;
	cb[i] = 0;
	
	if (cb_input_sz <= 85) space_for_en_in_coinbase = true;
	
	if (space_for_en_in_coinbase) {
		cb1idx[0] += append_bitcoin_varint_hex(cb_input_sz+15, &s->coinbase.coinb1[cb1idx[0]]); 
	} else {
		cb1idx[0] += append_bitcoin_varint_hex(cb_input_sz, &s->coinbase.coinb1[cb1idx[0]]);
	}
	memcpy(&s->coinbase.coinb1[cb1idx[0]], &cb[0], cb_input_sz*2);
	s->target_pot_index = target_pot_index + (cb1idx[0]>>1); 
	cb1idx[0] += cb_input_sz*2;
	
	if (space_for_en_in_coinbase) {
		uchar_to_hex(&s->coinbase.coinb1[cb1idx[0]], 0x0E);
		cb1idx[0]+=2;
		cb1idx[0] += sprintf(&s->coinbase.coinb1[cb1idx[0]], "%04" PRIx16, s->enprefix);
	} else {
		pk_u64le(s->coinbase.coinb1, cb1idx[0], 0x6666666666666666ULL);  // "ffffffff"
		cb1idx[0] += 8;
	}
	s->coinbase.coinb1[cb1idx[0]] = 0;
	
	if (space_for_en_in_coinbase) {
		pk_u64le(s->coinbase.coinb2, 0, 0x6666666666666666ULL); 
		cb2idx[0] = 8;
		cb2idx[0] += append_bitcoin_varint_hex(2, &s->coinbase.coinb2[cb2idx[0]]); 
	} else {
		cb1idx[0] += append_bitcoin_varint_hex(3, &s->coinbase.coinb1[cb1idx[0]]); 
		cb1idx[0] += sprintf(&s->coinbase.coinb1[cb1idx[0]], "0000000000000000106a0e%04" PRIx16, s->enprefix);
	}
	
	cb2idx[0] += sprintf(&s->coinbase.coinb2[cb2idx[0]], "%016llx", (unsigned long long)__builtin_bswap64(s->coinbase_value)); 
	cb2idx[0] += append_bitcoin_varint_hex(s->pool_addr_script_len, &s->coinbase.coinb2[cb2idx[0]]); 
	for(i=0;i<s->pool_addr_script_len;i++) {
		uchar_to_hex(&s->coinbase.coinb2[cb2idx[0]], s->pool_addr_script[i]);
		cb2idx[0]+=2;
	}
	
	cb2idx[0] += sprintf(&s->coinbase.coinb2[cb2idx[0]], "0000000000000000%2.2x%s", (unsigned int)strlen(s->block_template->default_witness_commitment)>>1, s->block_template->default_witness_commitment);
	cb2idx[0] += sprintf(&s->coinbase.coinb2[cb2idx[0]], "00000000");
	
	i = strlen(s->coinbase.coinb1); s->coinbase.coinb1_len = 0;
	for(j=0;j<i;j+=2) { s->coinbase.coinb1_bin[j>>1] = hex2bin_uchar(&s->coinbase.coinb1[j]); s->coinbase.coinb1_len++; }
	
	i = strlen(s->coinbase.coinb2); s->coinbase.coinb2_len = 0;
	for(j=0;j<i;j+=2) { s->coinbase.coinb2_bin[j>>1] = hex2bin_uchar(&s->coinbase.coinb2[j]); s->coinbase.coinb2_len++; }
}
