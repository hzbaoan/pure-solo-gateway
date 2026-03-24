/*
 *
 * DATUM Gateway
 * Decentralized Alternative Templates for Universal Mining
 *
 * This file is part of OCEAN's Bitcoin mining decentralization
 * project, DATUM.
 *
 * https://ocean.xyz
 *
 * ---
 *
 * Copyright (c) 2024-2025 Bitcoin Ocean, LLC & Jason Hughes
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
 * CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <errno.h>
#include <jansson.h>
#include <inttypes.h>
#include <curl/curl.h>
#include <time.h>

#include "datum_gateway.h"
#include "datum_jsonrpc.h"
#include "datum_utils.h"
#include "datum_blocktemplates.h"
#include "datum_conf.h"
#include "datum_stratum.h"
#include "datum_zmq.h"

static pthread_mutex_t new_notify_lock = PTHREAD_MUTEX_INITIALIZER;
static char new_notify_blockhash[72] = { 0 };
static int new_notify_height = 0;
static uint64_t new_notify_seq = 0;

#define DATUM_MAX_TEMPLATE_TXNS 16383
#define DATUM_TEMPLATE_FALLBACK_POLL_MS 100
#define DATUM_TEMPLATE_PRIORITY_RETRY_TIMEOUT_MS 4000

void datum_blocktemplates_notifynew(const char * const prevhash, const int height) {
	bool same_blockhash = false;

	pthread_mutex_lock(&new_notify_lock);
	if (prevhash && prevhash[0]) {
		same_blockhash = strcmp(prevhash, new_notify_blockhash) == 0;
		strncpy(new_notify_blockhash, prevhash, sizeof(new_notify_blockhash) - 1);
		new_notify_blockhash[sizeof(new_notify_blockhash) - 1] = 0;
	} else {
		new_notify_blockhash[0] = 0;
	}

	if (height > 0) {
		new_notify_height = height;
	} else if (!same_blockhash) {
		new_notify_height = 0;
	}
	new_notify_seq++;
	pthread_mutex_unlock(&new_notify_lock);
}

static void datum_blocktemplates_get_notify_state(char *notify_blockhash_out, size_t notify_blockhash_len, int *notify_height_out, uint64_t *notify_seq_out) {
	pthread_mutex_lock(&new_notify_lock);
	if (notify_blockhash_out && notify_blockhash_len) {
		strncpy(notify_blockhash_out, new_notify_blockhash, notify_blockhash_len - 1);
		notify_blockhash_out[notify_blockhash_len - 1] = 0;
	}
	if (notify_height_out) {
		*notify_height_out = new_notify_height;
	}
	if (notify_seq_out) {
		*notify_seq_out = new_notify_seq;
	}
	pthread_mutex_unlock(&new_notify_lock);
}

static void datum_blocktemplates_clear_notify_state_if_seq(const uint64_t notify_seq) {
	pthread_mutex_lock(&new_notify_lock);
	if (new_notify_seq == notify_seq) {
		new_notify_blockhash[0] = 0;
		new_notify_height = 0;
	}
	pthread_mutex_unlock(&new_notify_lock);
}

T_DATUM_TEMPLATE_DATA *template_data = NULL;

int next_template_index = 0;
static uint32_t template_slot_data_size = 0;

const char *datum_blocktemplates_error = NULL;

int datum_template_init(void) {
	char *temp = NULL, *ptr = NULL;
	int i,j;
	
	template_data = (T_DATUM_TEMPLATE_DATA *)calloc(sizeof(T_DATUM_TEMPLATE_DATA),MAX_TEMPLATES_IN_MEMORY+1);
	if (!template_data) {
		DLOG_FATAL("Could not allocate RAM for in-memory template data. :( (1)");
		return -1;
	}
	
	// TODO: Be smarter about dependent RAM data and size
	// we keep the tx metadata plus submitblock-ready hex for all transactions, with extra headroom
	j = (sizeof(T_DATUM_TEMPLATE_TXN) * (DATUM_MAX_TEMPLATE_TXNS + 1)) + (MAX_BLOCK_SIZE_BYTES * 3) + 2000000;
	temp = calloc(j, MAX_TEMPLATES_IN_MEMORY);
	if (!temp) {
		DLOG_FATAL("ERROR: Could not allocate RAM for in-memory template data. :( (2)");
		return -2;
	}
	
	ptr = temp;
	for(i=0;i<MAX_TEMPLATES_IN_MEMORY;i++) {
		template_data[i].local_data = ptr;
		ptr+=j;
		template_data[i].local_data_size = j;
		template_data[i].local_index = i;
		template_data[i].heap_allocated = false;
	}
	template_slot_data_size = (uint32_t)j;
	
	DLOG_DEBUG("Allocated %d MB of RAM for template memory", (j*MAX_TEMPLATES_IN_MEMORY)/(1024*1024));
	
	return 1;
}

void datum_template_acquire(T_DATUM_TEMPLATE_DATA *t) {
	if (!t) return;
	__atomic_add_fetch(&t->refcount, 1, __ATOMIC_RELAXED);
}

void datum_template_release(T_DATUM_TEMPLATE_DATA *t) {
	if (!t) return;
	if (__atomic_sub_fetch(&t->refcount, 1, __ATOMIC_ACQ_REL) != 0) return;
	if (!t->heap_allocated) return;
	free(t->local_data);
	free(t);
}

void datum_template_clear(T_DATUM_TEMPLATE_DATA* p) {
	p->coinbasevalue = 0;
	p->txn_count = 0;
	p->txn_total_size = 0;
	p->txn_data_offset = 0;
	p->txn_total_weight = 0;
	p->txn_total_sigops = 0;
	p->txns = p->local_data;
}

static T_DATUM_TEMPLATE_DATA *datum_template_alloc_heap_slot(void) {
	T_DATUM_TEMPLATE_DATA *p;
	
	p = calloc(1, sizeof(T_DATUM_TEMPLATE_DATA));
	if (!p) {
		DLOG_FATAL("Could not allocate overflow template slot metadata.");
		return NULL;
	}
	p->local_data = calloc(1, template_slot_data_size);
	if (!p->local_data) {
		DLOG_FATAL("Could not allocate overflow template slot payload.");
		free(p);
		return NULL;
	}
	p->local_data_size = template_slot_data_size;
	p->heap_allocated = true;
	p->local_index = UINT16_MAX;
	datum_template_clear(p);
	__atomic_store_n(&p->refcount, 1, __ATOMIC_RELEASE);
	DLOG_WARN("Template pool exhausted; allocated overflow template slot %p", p);
	return p;
}

T_DATUM_TEMPLATE_DATA *get_next_template_ptr(void) {
	T_DATUM_TEMPLATE_DATA *p;
	int i, idx;
	
	if (!template_data) return NULL;
	
	for(i=0;i<MAX_TEMPLATES_IN_MEMORY;i++) {
		idx = next_template_index + i;
		if (idx >= MAX_TEMPLATES_IN_MEMORY) idx -= MAX_TEMPLATES_IN_MEMORY;
		p = &template_data[idx];
		if (__atomic_load_n(&p->refcount, __ATOMIC_ACQUIRE) != 0) continue;
		datum_template_clear(p);
		__atomic_store_n(&p->refcount, 1, __ATOMIC_RELEASE);
		next_template_index = idx + 1;
		if (next_template_index >= MAX_TEMPLATES_IN_MEMORY) {
			next_template_index = 0;
		}
		return p;
	}
	
	return datum_template_alloc_heap_slot();
}

T_DATUM_TEMPLATE_DATA *datum_gbt_parser(json_t *gbt) {
	T_DATUM_TEMPLATE_DATA *tdata;
	const char *s;
	int i;
	json_t *tx_array;
#define DATUM_GBT_FAIL(...) do { DLOG_ERROR(__VA_ARGS__); datum_template_release(tdata); return NULL; } while(0)
	
	tdata = get_next_template_ptr();
	if (!tdata) {
		DLOG_ERROR("Could not get a template pointer.");
		return NULL;
	}
	
	tdata->height = json_integer_value(json_object_get(gbt, "height"));
	if (!tdata->height) {
		DATUM_GBT_FAIL("Missing data from GBT JSON (height)");
	}
	
	tdata->coinbasevalue = json_integer_value(json_object_get(gbt, "coinbasevalue"));
	if (!tdata->coinbasevalue) {
		DATUM_GBT_FAIL("Missing data from GBT JSON (coinbasevalue)");
	}
	
	tdata->mintime = json_integer_value(json_object_get(gbt, "mintime"));
	if (!tdata->mintime) {
		DATUM_GBT_FAIL("Missing data from GBT JSON (mintime)");
	}
	
	tdata->sigoplimit = json_integer_value(json_object_get(gbt, "sigoplimit"));
	if (!tdata->sigoplimit) {
		DATUM_GBT_FAIL("Missing data from GBT JSON (sigoplimit)");
	}
	
	tdata->curtime = json_integer_value(json_object_get(gbt, "curtime"));
	if (!tdata->curtime) {
		DATUM_GBT_FAIL("Missing data from GBT JSON (curtime)");
	}
	
	tdata->sizelimit = json_integer_value(json_object_get(gbt, "sizelimit"));
	if (!tdata->sizelimit) {
		DATUM_GBT_FAIL("Missing data from GBT JSON (sizelimit)");
	}
	
	tdata->weightlimit = json_integer_value(json_object_get(gbt, "weightlimit"));
	if (!tdata->weightlimit) {
		DATUM_GBT_FAIL("Missing data from GBT JSON (weightlimit)");
	}
	
	tdata->version = json_integer_value(json_object_get(gbt, "version"));
	if (!tdata->version) {
		DATUM_GBT_FAIL("Missing data from GBT JSON (version)");
	}
	
	s = json_string_value(json_object_get(gbt, "bits"));
	if (!s) {
		DATUM_GBT_FAIL("Missing data from GBT JSON (bits)");
	}
	if (strlen(s) != 8) {
		DATUM_GBT_FAIL("Wrong bits length from GBT JSON");
	}
	strcpy(tdata->bits, s);
	
	s = json_string_value(json_object_get(gbt, "previousblockhash"));
	if (!s) {
		DATUM_GBT_FAIL("Missing data from GBT JSON (previousblockhash)");
	}
	strncpy(tdata->previousblockhash, s, 71);
	
	s = json_string_value(json_object_get(gbt, "target"));
	if (!s) {
		DATUM_GBT_FAIL("Missing data from GBT JSON (target)");
	}
	
	s = json_string_value(json_object_get(gbt, "default_witness_commitment"));
	if (!s) {
		DATUM_GBT_FAIL("Missing data from GBT JSON (default_witness_commitment)");
	}
	strncpy(tdata->default_witness_commitment, s, 95);
	
	// "20000000", "192e17d5", "66256be5"
	// version, bits, time
	// 192e17d5 // gbt format matches stratum for bits
	
	// stash useful binary versions of prevblockhash and nbits
	for(i=0;i<64;i+=2) {
		tdata->previousblockhash_bin[31-(i>>1)] = hex2bin_uchar(&tdata->previousblockhash[i]);
	}
	for(i=0;i<4;i++) {
		tdata->bits_bin[3-i] = hex2bin_uchar(&tdata->bits[i<<1]);
	}
	tdata->bits_uint = upk_u32le(tdata->bits_bin, 0);
	nbits_to_target(tdata->bits_uint, tdata->block_target);
	
	// Get the txns
	tx_array = json_object_get(gbt, "transactions");
	if (!json_is_array(tx_array)) {
		DATUM_GBT_FAIL("Missing data from GBT JSON (transactions)");
	}
	
	tdata->txn_count = json_array_size(tx_array);
	if (tdata->txn_count > DATUM_MAX_TEMPLATE_TXNS) {
		DATUM_GBT_FAIL("Template has %u transactions, exceeding this build's supported maximum of %u. Rejecting template instead of truncating it.", tdata->txn_count, (unsigned int)DATUM_MAX_TEMPLATE_TXNS);
	}
	tdata->txn_data_offset = sizeof(T_DATUM_TEMPLATE_TXN)*tdata->txn_count;
	if (tdata->txn_count > 0) {
		for(i=0;i<tdata->txn_count;i++) {
			json_t *tx = json_array_get(tx_array, i);
			if (!tx) {
				DATUM_GBT_FAIL("transaction %d not found!", i);
			}
			if (!json_is_object(tx)) {
				DATUM_GBT_FAIL("transaction %d is not an object!", i);
			}
			
			// index (1 based, like GBT depends)
			tdata->txns[i].index_raw = i+1;
			
			// txid
			s = json_string_value(json_object_get(tx, "txid"));
			if (!s) {
				DATUM_GBT_FAIL("Missing data from GBT JSON transactions[%d] (txid)",i);
			}
			strcpy(tdata->txns[i].txid_hex, s);
			hex_to_bin_le(tdata->txns[i].txid_hex, tdata->txns[i].txid_bin);
			
			// hash
			s = json_string_value(json_object_get(tx, "hash"));
			if (!s) {
				DATUM_GBT_FAIL("Missing data from GBT JSON transactions[%d] (hash)",i);
			}
			
			// fee
			tdata->txns[i].fee_sats = json_integer_value(json_object_get(tx, "fee"));
			
			// sigops
			tdata->txns[i].sigops = json_integer_value(json_object_get(tx, "sigops"));
			
			// weight
			tdata->txns[i].weight = json_integer_value(json_object_get(tx, "weight"));
			
			// data
			s = json_string_value(json_object_get(tx, "data"));
			if (!s) {
				DATUM_GBT_FAIL("Missing data from GBT JSON transactions[%d] (data)",i);
			}
			
			// size
			tdata->txns[i].size = strlen(s)>>1;
			
			// raw txn data hex
			tdata->txns[i].txn_data_hex = &((char *)tdata->local_data)[tdata->txn_data_offset];
			tdata->txn_data_offset += (tdata->txns[i].size*2)+2;
			if (tdata->txn_data_offset >= tdata->local_data_size) {
				DATUM_GBT_FAIL("Exceeded template local size with txn data!");
			}
			strcpy(tdata->txns[i].txn_data_hex, s);
			
			// tallies
			tdata->txn_total_weight+=tdata->txns[i].weight;
			tdata->txn_total_size+=tdata->txns[i].size;
			tdata->txn_total_sigops+=tdata->txns[i].sigops;
		}
	}
	
#undef DATUM_GBT_FAIL
	return tdata;
}

static bool bitcoind_get_bestblockhash(CURL *tcurl, char *blockhash_out, size_t blockhash_out_len) {
	char req[160];
	json_t *reply = NULL;
	json_t *res = NULL;
	const char *blockhash = NULL;
	
	if (!tcurl || !blockhash_out || blockhash_out_len < 65) return false;
	blockhash_out[0] = 0;
	
	snprintf(req, sizeof(req), "{\"jsonrpc\":\"1.0\",\"id\":\"%" PRIu64 "\",\"method\":\"getbestblockhash\",\"params\":[]}", current_time_millis());
	reply = bitcoind_json_rpc_call(tcurl, &datum_config, req);
	if (!reply) return false;
	
	res = json_object_get(reply, "result");
	if (!json_is_string(res)) {
		json_decref(reply);
		return false;
	}
	
	blockhash = json_string_value(res);
	if (!blockhash || !blockhash[0]) {
		json_decref(reply);
		return false;
	}
	
	snprintf(blockhash_out, blockhash_out_len, "%s", blockhash);
	json_decref(reply);
	return true;
}

void *datum_gateway_template_thread(void *args) {
	CURL *tcurl = NULL;
	json_t *gbt = NULL, *res_val;
	uint64_t i = 0;
	char gbt_req[1024];
	T_DATUM_TEMPLATE_DATA *t;
	bool priority_fetch = false;
	uint64_t last_block_change = 0;
	uint64_t next_standard_fetch_tsms = 0;
	uint64_t priority_fetch_started_tsms = 0;
	int priority_retry_count = 0;
	pthread_t pthread_datum_zmq_hashblock;
	const uint64_t standard_refresh_ms = (uint64_t)datum_config.bitcoind_work_update_seconds * (uint64_t)1000;
	char current_prevhash[72];
	char notify_blockhash[72];
	char expected_blockhash[72];
	int notify_height = 0;
	int expected_height = 0;
	uint64_t notify_seq = 0;
	uint64_t expected_notify_seq = 0;
	uint64_t last_processed_notify_seq = 0;

	(void)args;
	tcurl = curl_easy_init();
	if (!tcurl) {
		DLOG_FATAL("Could not initialize cURL");
		panic_from_thread(__LINE__);
	}
	
	if (datum_template_init() < 1) {
		DLOG_FATAL("Couldn't setup template processor.");
		panic_from_thread(__LINE__);
	}
	
{
		unsigned char dummy[64];
    if (!addr_2_output_script(datum_config.mining_pool_address, &dummy[0], 64)) {
        DLOG_FATAL("Could not generate output script for pool addr! Perhaps invalid? This is bad.");
        panic_from_thread(__LINE__);
   }
	}

	if (datum_config.bitcoind_zmq_hashblock_url[0]) {
		DLOG_DEBUG("Starting ZMQ hashblock subscriber");
		pthread_create(&pthread_datum_zmq_hashblock, NULL, datum_zmq_hashblock_thread, NULL);
	}
	
	DLOG_DEBUG("Template fetcher thread ready.");

	current_prevhash[0] = 0;
	expected_blockhash[0] = 0;
	
	while(1) {
		bool should_fetch = false;
		bool standard_fetch_due = false;
		const uint64_t loop_start_tsms = current_time_millis();

		datum_blocktemplates_get_notify_state(notify_blockhash, sizeof(notify_blockhash), &notify_height, &notify_seq);
		if (notify_seq > last_processed_notify_seq && expected_notify_seq != notify_seq) {
			priority_fetch = true;
			priority_fetch_started_tsms = loop_start_tsms;
			priority_retry_count = 0;
			strncpy(expected_blockhash, notify_blockhash, sizeof(expected_blockhash) - 1);
			expected_blockhash[sizeof(expected_blockhash) - 1] = 0;
			expected_height = notify_height;
			expected_notify_seq = notify_seq;
			DLOG_INFO("Template notify seq %" PRIu64 " received, checking for new work", expected_notify_seq);
		}

		if (!priority_fetch && current_prevhash[0]) {
			char bestblockhash[72];

			if (bitcoind_get_bestblockhash(tcurl, bestblockhash, sizeof(bestblockhash)) && strcmp(bestblockhash, current_prevhash) != 0) {
				strncpy(expected_blockhash, bestblockhash, sizeof(expected_blockhash) - 1);
				expected_blockhash[sizeof(expected_blockhash) - 1] = 0;
				expected_height = 0;
				expected_notify_seq = 0;
				priority_fetch = true;
				priority_fetch_started_tsms = loop_start_tsms;
				priority_retry_count = 0;
				DLOG_INFO("100ms fallback poll detected new chain tip: %s", bestblockhash);
			}
		}

		if (priority_fetch) {
			should_fetch = true;
		} else if (!next_standard_fetch_tsms || loop_start_tsms >= next_standard_fetch_tsms) {
			should_fetch = true;
			standard_fetch_due = true;
		}

		if (!should_fetch) {
			usleep(DATUM_TEMPLATE_FALLBACK_POLL_MS * 1000);
			continue;
		}
		
		// fetch latest template
		i++;
		snprintf(gbt_req, sizeof(gbt_req), "{\"method\":\"getblocktemplate\",\"params\":[{\"rules\":[\"segwit\"]}],\"id\":%"PRIu64"}",(uint64_t)((uint64_t)time(NULL)<<(uint64_t)8)|(uint64_t)(i&255));
		gbt = bitcoind_json_rpc_call(tcurl, &datum_config, gbt_req);
		
		if (!gbt) {
			datum_blocktemplates_error = "Could not fetch new template!";
			DLOG_ERROR("Could not fetch new template from %s!", datum_config.bitcoind_rpcurl);
			sleep(1);
			continue;
		} else {
			res_val = json_object_get(gbt, "result");
			if (!res_val) {
				datum_blocktemplates_error = "Could not decode GBT result!";
				DLOG_ERROR("%s", datum_blocktemplates_error);
			} else {
				DLOG_DEBUG("DEBUG: calling datum_gbt_parser (priority=%d)", priority_fetch?1:0);
				t = datum_gbt_parser(res_val);
				
				if (t) {
					bool template_consumed = false;
					const uint64_t now_tsms = current_time_millis();
					const bool new_block = strcmp(t->previousblockhash, current_prevhash) != 0;
					datum_blocktemplates_error = NULL;
					DLOG_DEBUG("height: %lu / value: %"PRIu64, (unsigned long)t->height, t->coinbasevalue);
					DLOG_DEBUG("--- prevhash: %s", t->previousblockhash);
					DLOG_DEBUG("--- txn_count: %u / sigops: %u / weight: %u / size: %u", t->txn_count, t->txn_total_sigops, t->txn_total_weight, t->txn_total_size);
					
					if (new_block) {
						last_block_change = now_tsms;
						strcpy(current_prevhash, t->previousblockhash);
						priority_fetch = false;
						priority_fetch_started_tsms = 0;
						priority_retry_count = 0;
						if (expected_notify_seq > last_processed_notify_seq) {
							last_processed_notify_seq = expected_notify_seq;
						}
						DLOG_INFO("NEW NETWORK BLOCK: %s (%lu)", t->previousblockhash, (unsigned long)t->height);

						i = datum_stratum_v1_global_subscriber_count();
						DLOG_INFO("Updating priority stratum job for block %lu: %.8f BTC, %lu txns, %lu bytes (Sent to %llu stratum client%s)", (unsigned long)t->height, (double)t->coinbasevalue / (double)100000000.0, (unsigned long)t->txn_count, (unsigned long)t->txn_total_size, (unsigned long long)i, (i!=1)?"s":"");
						update_stratum_job(t,true,true,JOB_STATE_FULL_PRIORITY);
						template_consumed = true;
						datum_template_release(t);
						if (expected_notify_seq) {
							datum_blocktemplates_clear_notify_state_if_seq(expected_notify_seq);
						}
						expected_blockhash[0] = 0;
						expected_height = 0;
						expected_notify_seq = 0;
						next_standard_fetch_tsms = now_tsms + standard_refresh_ms;
					} else {
						if (priority_fetch) {
							if (expected_blockhash[0] && !strcmp(t->previousblockhash, expected_blockhash)) {
								if ((expected_height <= 0) || (expected_height == (int)t->height)) {
									priority_fetch = false;
									priority_fetch_started_tsms = 0;
									priority_retry_count = 0;
								}
							}
							if (!priority_fetch) {
								DLOG_DEBUG("Multi notified for block we knew details about. (%s)", expected_blockhash[0] ? expected_blockhash : "no hash");
								if (expected_notify_seq > last_processed_notify_seq) {
									last_processed_notify_seq = expected_notify_seq;
								}
								if (expected_notify_seq) {
									datum_blocktemplates_clear_notify_state_if_seq(expected_notify_seq);
								}
								expected_blockhash[0] = 0;
								expected_height = 0;
								expected_notify_seq = 0;
								next_standard_fetch_tsms = now_tsms + standard_refresh_ms;
							} else if (expected_notify_seq && !expected_blockhash[0]) {
								DLOG_DEBUG("Notify seq %" PRIu64 " had no expected blockhash; stopping priority fetch", expected_notify_seq);
								priority_fetch = false;
								priority_fetch_started_tsms = 0;
								priority_retry_count = 0;
								last_processed_notify_seq = expected_notify_seq;
								datum_blocktemplates_clear_notify_state_if_seq(expected_notify_seq);
								expected_blockhash[0] = 0;
								expected_height = 0;
								expected_notify_seq = 0;
								next_standard_fetch_tsms = now_tsms + standard_refresh_ms;
							} else if (expected_blockhash[0] && ((now_tsms - 2500) < last_block_change)) {
								DLOG_DEBUG("This is probably a duplicate signal, since we just changed blocks less than 2.5s ago");
								priority_fetch = false;
								priority_fetch_started_tsms = 0;
								priority_retry_count = 0;
								if (expected_notify_seq > last_processed_notify_seq) {
									last_processed_notify_seq = expected_notify_seq;
								}
								if (expected_notify_seq) {
									datum_blocktemplates_clear_notify_state_if_seq(expected_notify_seq);
								}
								expected_blockhash[0] = 0;
								expected_height = 0;
								expected_notify_seq = 0;
								next_standard_fetch_tsms = now_tsms + standard_refresh_ms;
							} else if (!expected_notify_seq && ((now_tsms - priority_fetch_started_tsms) >= DATUM_TEMPLATE_PRIORITY_RETRY_TIMEOUT_MS)) {
								DLOG_DEBUG("Fallback priority fetch timed out waiting for block %s", expected_blockhash[0] ? expected_blockhash : "(none)");
								priority_fetch = false;
								priority_fetch_started_tsms = 0;
								priority_retry_count = 0;
								expected_blockhash[0] = 0;
								expected_height = 0;
								next_standard_fetch_tsms = now_tsms + standard_refresh_ms;
							} else if (expected_notify_seq && expected_blockhash[0] && ((now_tsms - priority_fetch_started_tsms) >= DATUM_TEMPLATE_PRIORITY_RETRY_TIMEOUT_MS)) {
								char pending_notify_blockhash[72];
								int pending_notify_height = 0;
								uint64_t pending_notify_seq = 0;
								bool pending_notify_matches_expected = false;

								datum_blocktemplates_get_notify_state(pending_notify_blockhash, sizeof(pending_notify_blockhash), &pending_notify_height, &pending_notify_seq);
								pending_notify_matches_expected = strcmp(pending_notify_blockhash, expected_blockhash) == 0
									&& ((expected_height <= 0) || (pending_notify_height == expected_height));
								if ((pending_notify_seq == expected_notify_seq) && (pending_notify_seq > last_processed_notify_seq) && pending_notify_matches_expected) {
									DLOG_WARN("We received template notify seq %" PRIu64 ", however after %d attempts / %d ms we did not see the expected block.", expected_notify_seq, priority_retry_count + 1, DATUM_TEMPLATE_PRIORITY_RETRY_TIMEOUT_MS);
									priority_fetch = false;
									priority_fetch_started_tsms = 0;
									priority_retry_count = 0;
									last_processed_notify_seq = expected_notify_seq;
									datum_blocktemplates_clear_notify_state_if_seq(expected_notify_seq);
									expected_blockhash[0] = 0;
									expected_height = 0;
									expected_notify_seq = 0;
									next_standard_fetch_tsms = now_tsms + standard_refresh_ms;
								}
							} else {
								DLOG_DEBUG("Priority fetch pending (notify_seq=%" PRIu64 "), however new = %s, t->previousblockhash = %s, t->height = %lu, new_notify_height = %d", expected_notify_seq, expected_blockhash[0] ? expected_blockhash : "(none)", t->previousblockhash, (unsigned long)t->height, expected_height);
								
								priority_retry_count++;
							}
						} else if (standard_fetch_due) {
							strcpy(current_prevhash, t->previousblockhash);
							i = datum_stratum_v1_global_subscriber_count();
							DLOG_INFO("Updating standard stratum job for block %lu: %.8f BTC, %lu txns, %lu bytes (Sent to %llu stratum client%s)", (unsigned long)t->height, (double)t->coinbasevalue / (double)100000000.0, (unsigned long)t->txn_count, (unsigned long)t->txn_total_size, (unsigned long long)i, (i!=1)?"s":"");
							update_stratum_job(t,false,false,JOB_STATE_FULL_NORMAL);
							template_consumed = true;
							datum_template_release(t);
							next_standard_fetch_tsms = now_tsms + standard_refresh_ms;
						}
					}
					if (!template_consumed) datum_template_release(t);
				}
			}
			json_decref(gbt);
		}
		gbt = NULL;
		if (priority_fetch) {
			usleep(DATUM_TEMPLATE_FALLBACK_POLL_MS * 1000);
		}
	}
	// this thread is never intended to exit unless the application dies
	
	// TODO: Clean things up
}
