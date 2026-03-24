/*
 * Pure Solo Gateway
 * Refactored for Solo Mining Only
 */

#ifndef _DATUM_STRATUM_H_
#define _DATUM_STRATUM_H_

#include <stdbool.h>

#ifndef T_DATUM_CLIENT_DATA
	#include "datum_sockets.h"
#endif

#ifndef T_DATUM_TEMPLATE_DATA
	#include "datum_blocktemplates.h"
#endif

#define MAX_STRATUM_JOBS 256

// Submitblock json rpc command max size is max block size * 2 for ascii plus some breathing room
#define MAX_SUBMITBLOCK_SIZE 8500000

/////////////////////////////////
// Stratum job types
/////////////////////////////////

#define JOB_STATE_FULL_PRIORITY 3
#define JOB_STATE_FULL_NORMAL 4

typedef struct {
	char coinb1[STRATUM_COINBASE1_MAX_LEN];
	char coinb2[STRATUM_COINBASE2_MAX_LEN];
	unsigned char coinb1_bin[STRATUM_COINBASE1_MAX_LEN>>1];
	unsigned char coinb2_bin[STRATUM_COINBASE2_MAX_LEN>>1];
	
	int coinb1_len;
	int coinb2_len;
} T_DATUM_STRATUM_COINBASE;

typedef struct {
	int global_index;
	uint64_t refcount;
	uint64_t job_uid;
	uint64_t publish_generation;
	uint64_t block_generation;
	
	char job_id[24];
	char prevhash[68];
	unsigned char prevhash_bin[32];
	char version[10];
	uint32_t version_uint;
	char nbits[10];
	unsigned char nbits_bin[4];
	uint32_t nbits_uint;
	char ntime[10];
	
	unsigned char block_target[32];
	
	T_DATUM_TEMPLATE_DATA *block_template;
	
	unsigned char merklebranch_count;
	char merklebranches_hex[24][72];
	unsigned char merklebranches_bin[24][32];
	
	char merklebranches_full[4096];
	
	unsigned char pool_addr_script[64];
	int pool_addr_script_len;
	
	T_DATUM_STRATUM_COINBASE coinbase;
	int target_pot_index; 
	
	uint64_t coinbase_value;
	uint64_t height;
	uint16_t enprefix;
	
	uint64_t tsms; 
	
	bool clean;
	
	int job_state;
} T_DATUM_STRATUM_JOB;

typedef struct {
	char notify_job_id[24];
	T_DATUM_STRATUM_JOB *job;
	uint8_t target[32];
	uint64_t diff;
	uint64_t tsms;
} T_DATUM_MINER_JOB_HISTORY_ENTRY;

typedef struct T_DATUM_STRATUM_THREADPOOL_DATA {
	T_DATUM_STRATUM_JOB *cur_stratum_job;
	uint64_t latest_stratum_job_generation;
	bool new_job;
	uint64_t loop_tsms;
	
	int notify_remaining_count;
	uint64_t notify_start_time;
	uint64_t notify_last_time;
	uint64_t notify_delay_per_slot_tsms;
	int notify_last_cid;
	uint64_t next_kick_check_tsms;
	
	char submitblock_req[MAX_SUBMITBLOCK_SIZE];
	
	void *dupes;
} T_DATUM_STRATUM_THREADPOOL_DATA;

typedef struct {
	unsigned char active_index; 
	
	uint64_t last_swap_tsms; 
	uint64_t last_swap_ms; 
	
	uint64_t diff_accepted[2];
	
	uint64_t last_share_tsms;
} T_DATUM_STRATUM_USER_STATS;

typedef struct {
	uint32_t sid, sid_inv;
	uint64_t connect_tsms;
	char useragent[128];
	char last_auth_username[192];
	
	bool extension_version_rolling;
	uint32_t extension_version_rolling_mask;
	bool subscribed;
	uint64_t subscribe_tsms;
	
	uint64_t last_sent_diff;
	uint64_t current_diff;
	
	uint64_t share_diff_accepted;
	uint64_t share_count_accepted;
	
	uint64_t share_diff_rejected;
	uint64_t share_count_rejected;
	
	uint64_t share_count_since_snap;
	uint64_t share_diff_since_snap;
	uint64_t share_snap_tsms;
	
	bool quickdiff_active;
	
	double best_share_diff;
	
	uint64_t forced_high_min_diff;
	uint16_t next_notify_job_token;
	uint32_t job_history_count;
	uint32_t job_history_capacity;
	T_DATUM_MINER_JOB_HISTORY_ENTRY *job_history;
	
	T_DATUM_STRATUM_USER_STATS stats;
	
	T_DATUM_STRATUM_THREADPOOL_DATA *sdata;
} T_DATUM_MINER_DATA;

extern pthread_rwlock_t stratum_global_job_ptr_lock;

int send_mining_notify(T_DATUM_CLIENT_DATA *c, bool clean, bool quickdiff);
void update_stratum_job(T_DATUM_TEMPLATE_DATA *block_template, bool new_block, bool clean, int job_state);
void stratum_job_merkle_root_calc(T_DATUM_STRATUM_JOB *s, unsigned char *coinbase_txn_hash, unsigned char *merkle_root_output);
int assembleBlockAndSubmit(uint8_t *block_header, uint8_t *coinbase_txn, size_t coinbase_txn_size, T_DATUM_STRATUM_JOB *job, T_DATUM_STRATUM_THREADPOOL_DATA *sdata, const char *block_hash_hex);
int send_mining_set_difficulty(T_DATUM_CLIENT_DATA *c);
T_DATUM_STRATUM_JOB *datum_stratum_latest_job_acquire(void);
void datum_stratum_job_release(T_DATUM_STRATUM_JOB *job);
bool datum_stratum_job_is_stale_prevblock(const T_DATUM_STRATUM_JOB *job);

void *datum_stratum_v1_socket_server(void *arg);
void datum_stratum_v1_socket_thread_init(T_DATUM_THREAD_DATA *my);
void datum_stratum_v1_socket_thread_loop(T_DATUM_THREAD_DATA *my);
int datum_stratum_v1_socket_thread_client_cmd(T_DATUM_CLIENT_DATA *c, char *line);
void datum_stratum_v1_socket_thread_client_closed(T_DATUM_CLIENT_DATA *c, const char *msg);
void datum_stratum_v1_socket_thread_client_new(T_DATUM_CLIENT_DATA *c);
int datum_stratum_v1_global_subscriber_count(void);
double datum_stratum_v1_est_total_th_sec(void);
char* datum_stratum_get_workers_json(void);

extern T_DATUM_SOCKET_APP *global_stratum_app;

#endif
