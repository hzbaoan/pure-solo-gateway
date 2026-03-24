/*
 * Pure Solo Gateway
 * Refactored for Solo Mining Only
 */

#ifndef _DATUM_CONF_H_
#define _DATUM_CONF_H_

#define DATUM_CONFIG_MAX_ARRAY_ENTRIES 32
#define DATUM_MAX_BLOCK_SUBMITS DATUM_CONFIG_MAX_ARRAY_ENTRIES
#define DATUM_MAX_SUBMIT_URL_LEN 512

#include <stdbool.h>
#include <stdint.h>
#include <jansson.h>

enum datum_conf_vartype {
	DATUM_CONF_BOOL,
	DATUM_CONF_INT,
	DATUM_CONF_STRING,
	DATUM_CONF_STRING_ARRAY,
};

typedef struct {
	char category[32];
	char name[64];
	char description[512];
	const char *example;
	bool example_default;
	enum datum_conf_vartype var_type;
	union {
		int default_int;
		bool default_bool;
		struct {
			int max_string_len;
			const char *default_string[DATUM_CONFIG_MAX_ARRAY_ENTRIES];
		};
	};
	void *ptr;
	bool required;
} T_DATUM_CONFIG_ITEM;

const T_DATUM_CONFIG_ITEM *datum_config_get_option_info(const char *category, size_t category_len, const char *name, size_t name_len);
const T_DATUM_CONFIG_ITEM *datum_config_get_option_info2(const char *category, const char *name);

// Globally accessable config options
typedef struct {
	char bitcoind_rpcuserpass[256];
	char bitcoind_rpccookiefile[1024];
	char bitcoind_rpcuser[128];
	char bitcoind_rpcpassword[128];
	char bitcoind_rpcurl[256];
	int bitcoind_work_update_seconds;
	char bitcoind_zmq_hashblock_url[256];
	
	char stratum_v1_listen_addr[128];
	int stratum_v1_listen_port;
	int stratum_v1_max_clients;
	int stratum_v1_max_threads;
	int stratum_v1_max_clients_per_thread;
	int stratum_v1_trust_proxy;
	
	int stratum_v1_vardiff_min;
	int stratum_v1_vardiff_target_shares_min;
	int stratum_v1_vardiff_quickdiff_count;
	int stratum_v1_vardiff_quickdiff_delta;
	int stratum_v1_share_stale_seconds;
	bool stratum_v1_fingerprint_miners;
	int stratum_v1_idle_timeout_no_subscribe;
	int stratum_v1_idle_timeout_no_share;
	int stratum_v1_idle_timeout_max_last_work;
	
	char mining_pool_address[256];
	char mining_coinbase_tag_primary[64];
	char mining_coinbase_tag_secondary[64];
	char mining_save_submitblocks_dir[256];
	int coinbase_unique_id;
	
	char api_listen_addr[128];
	int api_listen_port;
	
	int extra_block_submissions_count;
	char extra_block_submissions_urls[DATUM_MAX_BLOCK_SUBMITS][DATUM_MAX_SUBMIT_URL_LEN];
	
	bool clog_to_file;
	bool clog_to_console;
	int clog_level_console;
	int clog_level_file;
	bool clog_calling_function;
	bool clog_to_stderr;
	bool clog_rotate_daily;
	char clog_file[1024];
	
} global_config_t;

extern global_config_t datum_config;

int datum_read_config(const char *conffile);
void datum_gateway_help(const char *argv0);
void datum_gateway_example_conf(void);

#endif
