/*
 * Pure Solo Gateway
 * Custom configurator and help output generator
 */

#include <assert.h>
#include <limits.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <jansson.h>

#include "datum_conf.h"
#include "datum_jsonrpc.h"
#include "datum_utils.h"
#include "datum_sockets.h"

global_config_t datum_config;

const char *datum_conf_var_type_text[] = {
	"boolean",
	"integer",
	"string",
	"string_array",
};

const T_DATUM_CONFIG_ITEM datum_config_options[] = {
	// Bitcoind configs
	{ .var_type = DATUM_CONF_STRING, 	.category = "bitcoind", 	.name = "rpccookiefile",			.description = "Path to file to read RPC cookie from, for communication with local bitcoind.",
		.required = false, .ptr = datum_config.bitcoind_rpccookiefile,			.default_string[0] = "", .max_string_len = sizeof(datum_config.bitcoind_rpccookiefile) },
	{ .var_type = DATUM_CONF_STRING, 	.category = "bitcoind", 	.name = "rpcuser",					.description = "RPC username for communication with local bitcoind.",
		.example = "\"admin\"",
		.required = false, .ptr = datum_config.bitcoind_rpcuser,			.default_string[0] = "", .max_string_len = sizeof(datum_config.bitcoind_rpcuser) },
	{ .var_type = DATUM_CONF_STRING, 	.category = "bitcoind", 	.name = "rpcpassword",				.description = "RPC password for communication with local bitcoind.",
		.example = "\"something only you know\"",
		.required = false, .ptr = datum_config.bitcoind_rpcpassword,			.default_string[0] = "", .max_string_len = sizeof(datum_config.bitcoind_rpcpassword) },
	{ .var_type = DATUM_CONF_STRING, 	.category = "bitcoind", 	.name = "rpcurl",					.description = "RPC URL for communication with local bitcoind. (GBT Template Source)",
		.example = "\"http://127.0.0.1:8332\"",
		.required = true, .ptr = datum_config.bitcoind_rpcurl, .max_string_len = sizeof(datum_config.bitcoind_rpcurl) },
	{ .var_type = DATUM_CONF_INT,	 	.category = "bitcoind", 	.name = "work_update_seconds",		.description = "How many seconds between normal work updates?  (5-120, 40 suggested)",
		.required = false, .ptr = &datum_config.bitcoind_work_update_seconds, .default_int = 40 },
	{ .var_type = DATUM_CONF_STRING, 	.category = "bitcoind", 	.name = "zmq_hashblock_url",		.description = "ZeroMQ address for bitcoind -zmqpubhashblock notifications. Used to trigger fast GBT refreshes after a new chain tip announcement.",
		.example = "\"tcp://127.0.0.1:28332\"",
		.required = false, .ptr = datum_config.bitcoind_zmq_hashblock_url,		.default_string[0] = "", .max_string_len = sizeof(datum_config.bitcoind_zmq_hashblock_url) },
	
	// stratum v1 server configs
	{ .var_type = DATUM_CONF_STRING, 	.category = "stratum", 		.name = "listen_addr",					.description = "IP address to listen for Stratum Gateway connections",
		.required = false, .ptr = datum_config.stratum_v1_listen_addr,				.default_string[0] = "", .max_string_len = sizeof(datum_config.stratum_v1_listen_addr) },
	{ .var_type = DATUM_CONF_INT, 		.category = "stratum", 		.name = "listen_port",				.description = "Listening port for Stratum Gateway",
		.example_default = true,
		.required = false, .ptr = &datum_config.stratum_v1_listen_port, 				.default_int = 3333 },
	{ .var_type = DATUM_CONF_INT, 		.category = "stratum", 		.name = "max_clients_per_thread",	.description = "Maximum clients per Stratum server thread",
		.required = false, .ptr = &datum_config.stratum_v1_max_clients_per_thread, 		.default_int = 128 },
	{ .var_type = DATUM_CONF_INT, 		.category = "stratum", 		.name = "max_threads",				.description = "Maximum Stratum server threads",
		.required = false, .ptr = &datum_config.stratum_v1_max_threads,					.default_int = 8 },
	{ .var_type = DATUM_CONF_INT, 		.category = "stratum", 		.name = "max_clients",				.description = "Maximum total Stratum clients before rejecting connections",
		.required = false, .ptr = &datum_config.stratum_v1_max_clients, 				.default_int = 1024 },
	{ .var_type = DATUM_CONF_INT, 		.category = "stratum", 		.name = "trust_proxy",		.description = "Enable support for the PROXY protocol, trusting up to the specified number of levels deep of proxies (-1 to disable entirely)",
		.required = false, .ptr = &datum_config.stratum_v1_trust_proxy, 	.default_int = -1 },
	{ .var_type = DATUM_CONF_INT, 		.category = "stratum", 		.name = "vardiff_min",				.description = "Work difficulty floor",
		.required = false, .ptr = &datum_config.stratum_v1_vardiff_min, 				.default_int = 16384 },
	{ .var_type = DATUM_CONF_INT, 		.category = "stratum", 		.name = "vardiff_target_shares_min",.description = "Adjust work difficulty to target this many shares per minute",
		.required = false, .ptr = &datum_config.stratum_v1_vardiff_target_shares_min, 	.default_int = 8 },
	{ .var_type = DATUM_CONF_INT, 		.category = "stratum", 		.name = "vardiff_quickdiff_count",	.description = "How many shares before considering a quick diff update",
		.required = false, .ptr = &datum_config.stratum_v1_vardiff_quickdiff_count, 	.default_int = 8 },
	{ .var_type = DATUM_CONF_INT, 		.category = "stratum", 		.name = "vardiff_quickdiff_delta",	.description = "How many times faster than our target does the miner have to be before we enforce a quick diff bump",
		.required = false, .ptr = &datum_config.stratum_v1_vardiff_quickdiff_delta, 	.default_int = 8 },
	{ .var_type = DATUM_CONF_INT, 		.category = "stratum", 		.name = "share_stale_seconds",		.description = "How many seconds after a job is generated before a share submission is considered stale?",
		.required = false, .ptr = &datum_config.stratum_v1_share_stale_seconds, 		.default_int = 120 },
	{ .var_type = DATUM_CONF_BOOL, 		.category = "stratum", 		.name = "fingerprint_miners",		.description = "Attempt to fingerprint miners for better use of coinbase space",
		.required = false, .ptr = &datum_config.stratum_v1_fingerprint_miners, 			.default_bool = true },
	{ .var_type = DATUM_CONF_INT, 		.category = "stratum", 		.name = "idle_timeout_no_subscribe",.description = "Seconds we allow a connection to be idle without seeing a work subscription? (0 disables)",
		.required = false, .ptr = &datum_config.stratum_v1_idle_timeout_no_subscribe, 	.default_int = 15 },
	{ .var_type = DATUM_CONF_INT, 		.category = "stratum", 		.name = "idle_timeout_no_shares",	.description = "Seconds we allow a subscribed connection to be idle without seeing at least one accepted share? (0 disables)",
		.required = false, .ptr = &datum_config.stratum_v1_idle_timeout_no_share, 	.default_int = 7200 },
	{ .var_type = DATUM_CONF_INT, 		.category = "stratum", 		.name = "idle_timeout_max_last_work",	.description = "Seconds we allow a subscribed connection to be idle since its last accepted share? (0 disables)",
		.required = false, .ptr = &datum_config.stratum_v1_idle_timeout_max_last_work, 	.default_int = 0 },
	
	// mining settings
	{ .var_type = DATUM_CONF_STRING, 	.category = "mining", 		.name = "pool_address",				.description = "Bitcoin address used for mining rewards.",
		.example = "\"put your own Bitcoin invoice address here\"",
		.required = true, .ptr = datum_config.mining_pool_address, .max_string_len = sizeof(datum_config.mining_pool_address) },
	{ .var_type = DATUM_CONF_STRING, 	.category = "mining", 		.name = "coinbase_tag_primary",		.description = "Text to have in the primary coinbase tag",
		.example_default = true,
		.required = false, .ptr = datum_config.mining_coinbase_tag_primary,				.default_string[0] = "Solo Gateway", .max_string_len = sizeof(datum_config.mining_coinbase_tag_primary) },
	{ .var_type = DATUM_CONF_STRING, 	.category = "mining", 		.name = "coinbase_tag_secondary",	.description = "Text to have in the secondary coinbase tag (Short name/identifier)",
		.example_default = true,
		.required = false, .ptr = datum_config.mining_coinbase_tag_secondary,			.default_string[0] = "Solo Miner", .max_string_len = sizeof(datum_config.mining_coinbase_tag_secondary) },
	{ .var_type = DATUM_CONF_INT, 		.category = "mining", 		.name = "coinbase_unique_id",		.description = "A unique ID between 1 and 65535. This is appended to the coinbase.",
		.required = false, .ptr = &datum_config.coinbase_unique_id, 		.default_int = 4242 },
	{ .var_type = DATUM_CONF_STRING, 	.category = "mining", 		.name = "save_submitblocks_dir",	.description = "Directory to save all submitted blocks to as submitblock JSON files",
		.required = false, .ptr = datum_config.mining_save_submitblocks_dir,			.default_string[0] = "", .max_string_len = sizeof(datum_config.mining_save_submitblocks_dir) },
	
	// API/dashboard (Simplified for Read-Only)
	{ .var_type = DATUM_CONF_STRING, 	.category = "api", 			.name = "listen_addr",					.description = "IP address to listen for API/dashboard requests",
		.required = false, .ptr = datum_config.api_listen_addr,				.default_string[0] = "", .max_string_len = sizeof(datum_config.api_listen_addr) },
	{ .var_type = DATUM_CONF_INT, 		.category = "api",	 		.name = "listen_port",				.description = "Port to listen for API/dashboard requests (0=disabled)",
		.example = "7152",
		.required = false, .ptr = &datum_config.api_listen_port, 						.default_int = 7152 }, // 默认开启控制台面板
	
	// extra block submissions list
	{ .var_type = DATUM_CONF_STRING_ARRAY, 	.category = "extra_block_submissions", 	.name = "urls",		.description = "Array of bitcoind RPC URLs to submit our blocks to directly.  Include auth info: http://user:pass@IP",
		.required = false, .ptr = datum_config.extra_block_submissions_urls[0],		.max_string_len = sizeof(*datum_config.extra_block_submissions_urls) },
	
	// logger
	{ .var_type = DATUM_CONF_BOOL, 		.category = "logger", 		.name = "log_to_console",			.description = "Enable logging of messages to the console",
		.example_default = true,
		.required = false, .ptr = &datum_config.clog_to_console, 	.default_bool = true },
	{ .var_type = DATUM_CONF_BOOL, 		.category = "logger", 		.name = "log_to_stderr",			.description = "Log console messages to stderr *instead* of stdout",
		.required = false, .ptr = &datum_config.clog_to_stderr, 	.default_bool = false },
	{ .var_type = DATUM_CONF_BOOL, 		.category = "logger", 		.name = "log_to_file",				.description = "Enable logging of messages to a file",
		.example_default = true,
		.required = false, .ptr = &datum_config.clog_to_file, 		.default_bool = false },
	{ .var_type = DATUM_CONF_STRING, 	.category = "logger", 		.name = "log_file",					.description = "Path to file to write log messages, when enabled",
		.example = "\"/var/log/datum.log\"",
		.required = false, .ptr = datum_config.clog_file,			.default_string[0] = "", .max_string_len = sizeof(datum_config.clog_file) },
	{ .var_type = DATUM_CONF_BOOL, 		.category = "logger", 		.name = "log_rotate_daily",			.description = "Rotate the message log file at midnight",
		.example_default = true,
		.required = false, .ptr = &datum_config.clog_rotate_daily, 		.default_bool = true },
	{ .var_type = DATUM_CONF_BOOL, 		.category = "logger", 		.name = "log_calling_function",		.description = "Log the name of the calling function when logging",
		.required = false, .ptr = &datum_config.clog_calling_function, 		.default_bool = true },
	{ .var_type = DATUM_CONF_INT, 		.category = "logger",	.name = "log_level_console",			.description = "Minimum log level for console messages (0=All, 1=Debug, 2=Info, 3=Warn, 4=Error, 5=Fatal)",
		.example_default = true,
		.required = false, .ptr = &datum_config.clog_level_console, .default_int = 2 },
	{ .var_type = DATUM_CONF_INT, 		.category = "logger",	.name = "log_level_file",				.description = "Minimum log level for log file messages (0=All, 1=Debug, 2=Info, 3=Warn, 4=Error, 5=Fatal)",
		.example_default = true,
		.required = false, .ptr = &datum_config.clog_level_file, .default_int = 1 },
};

#define NUM_CONFIG_ITEMS (sizeof(datum_config_options) / sizeof(datum_config_options[0]))

const T_DATUM_CONFIG_ITEM *datum_config_get_option_info(const char * const category, const size_t category_len, const char * const name, const size_t name_len) {
	for (size_t i = 0; i < NUM_CONFIG_ITEMS; ++i) {
		if (strncmp(category, datum_config_options[i].category, category_len)) continue;
		if (datum_config_options[i].category[category_len]) continue;
		if (strncmp(name, datum_config_options[i].name, name_len)) continue;
		if (datum_config_options[i].name[name_len]) continue;
		return &datum_config_options[i];
	}
	return NULL;
}

const T_DATUM_CONFIG_ITEM *datum_config_get_option_info2(const char * const category, const char * const name) {
	return datum_config_get_option_info(category, strlen(category), name, strlen(name));
}

json_t *load_json_from_file(const char *file_path) {
	json_error_t error;
	json_t *root = json_load_file(file_path, 0, &error);
	
	if(!root) {
		DLOG_ERROR("Error parsing JSON file: %s", error.text);
		return NULL;
	}
	return root;
}

void datum_config_set_default(const T_DATUM_CONFIG_ITEM *c) {
	switch(c->var_type) {
		case DATUM_CONF_INT: {
			*((int *)c->ptr) = c->default_int;
			break;
		}
		case DATUM_CONF_BOOL: {
			*((bool *)c->ptr) = c->default_bool;
			break;
		}
		case DATUM_CONF_STRING: {
			strncpy((char *)c->ptr, c->default_string[0], c->max_string_len-1);
			((char *)c->ptr)[c->max_string_len-1] = 0;
			break;
		}
		case DATUM_CONF_STRING_ARRAY: {
			((char *)c->ptr)[0] = 0;
			break;
		}
	}
}

int datum_config_parse_value(const T_DATUM_CONFIG_ITEM *c, json_t *item) {
	switch(c->var_type) {
		case DATUM_CONF_INT: {
			if (json_is_null(item)) {
				*((int *)c->ptr) = 0;
				return 1;
			}
			if (!json_is_integer(item)) return -1;
			const json_int_t value = json_integer_value(item);
			if (value > INT_MAX || value < INT_MIN) return -1;
			*((int *)c->ptr) = value;
			return 1;
		}
		case DATUM_CONF_BOOL: {
			if (json_is_null(item)) {
				*((bool *)c->ptr) = false;
				return 1;
			}
			if (!json_is_boolean(item)) return -1;
			*((bool *)c->ptr) = json_boolean_value(item)?true:false;
			return 1;
		}
		case DATUM_CONF_STRING: {
			if (json_is_null(item)) {
				((char *)c->ptr)[0] = 0;
				return 1;
			}
			if (!json_is_string(item)) return -1;
			int written = snprintf((char *)c->ptr, c->max_string_len, "%s", json_string_value(item));
			if (written >= c->max_string_len) {
				return -2;
			}
			return 1;
		}
		case DATUM_CONF_STRING_ARRAY: {
			if (!json_is_array(item)) return -1;
			size_t index;
			json_t *value;
			int i = 0;
			json_array_foreach(item, index, value) {
				if (!json_is_string(value)) return -1;
				if (i < (DATUM_CONFIG_MAX_ARRAY_ENTRIES-1)) {
					strncpy(((char (*)[1024])c->ptr)[i], json_string_value(value), c->max_string_len-1);
					((char (*)[1024])c->ptr)[i][c->max_string_len-1] = 0;
					i++;
				}
			}
			((char (*)[1024])c->ptr)[i][0] = 0;
			return 1;
		}
	}
	return -1;
}

static void datum_config_opt_missing_error(const T_DATUM_CONFIG_ITEM * const opt) {
	DLOG_ERROR("Required configuration option (%s.%s) not found in config file:", opt->category, opt->name);
	DLOG_ERROR("--- Config description: \"%s\"", opt->description);
}

int datum_read_config(const char *conffile) {
	json_t *config = NULL;
	json_t *cat, *item;
	unsigned int i;
	int j;
	
	memset(&datum_config, 0, sizeof(global_config_t));
	config = load_json_from_file(conffile);
	
	if (!json_is_object(config)) {
		DLOG_FATAL("Could not read configuration JSON file!");
		return -1;
	}
	
	for (i=0;i<NUM_CONFIG_ITEMS;i++) {
		item = NULL; cat = NULL;
		cat = json_object_get(config, datum_config_options[i].category);
		if (json_is_object(cat)) {
			item = json_object_get(cat, datum_config_options[i].name);
		}
		if ((!item) || json_is_null(item)) {
			if (datum_config_options[i].required) {
				datum_config_opt_missing_error(&datum_config_options[i]);
				return 0;
			} else {
				datum_config_set_default(&datum_config_options[i]);
			}
			continue;
		}
		
		j = datum_config_parse_value(&datum_config_options[i], item);
		if (j == -1) {
			DLOG_ERROR("Could not parse configuration option %s.%s. Type should be %s", datum_config_options[i].category, datum_config_options[i].name, datum_conf_var_type_text[datum_config_options[i].var_type]);
			return -1;
		} else if (j == -2) {
			DLOG_ERROR("Configuration option %s.%s exceeds maximum length of %d", datum_config_options[i].category, datum_config_options[i].name, datum_config_options[i].max_string_len - 1);
			return -1;
		}
	}
	
	json_decref(config);
	
	snprintf(datum_config.bitcoind_rpcuserpass, sizeof(datum_config.bitcoind_rpcuserpass), "%s:%s", datum_config.bitcoind_rpcuser, datum_config.bitcoind_rpcpassword);
	
	datum_logger_config(datum_config.clog_to_file, datum_config.clog_to_console, datum_config.clog_level_console, datum_config.clog_level_file, datum_config.clog_calling_function, datum_config.clog_to_stderr, datum_config.clog_rotate_daily, datum_config.clog_file);
	
	i = 0;
	for(i=0;i<DATUM_CONFIG_MAX_ARRAY_ENTRIES;i++) {
		if (datum_config.extra_block_submissions_urls[i][0] == 0) { break; }
	}
	datum_config.extra_block_submissions_count = i;
	
	if (datum_config.bitcoind_work_update_seconds < 5) {
		datum_config.bitcoind_work_update_seconds = 5;
	}
	if (datum_config.bitcoind_work_update_seconds > 120) {
		datum_config.bitcoind_work_update_seconds = 120;
	}
	
	if (datum_config.bitcoind_rpcuser[0]) {
		if (!datum_config.bitcoind_rpcpassword[0]) {
			datum_config_opt_missing_error(datum_config_get_option_info2("bitcoind", "rpcpassword"));
			return 0;
		}
		snprintf(datum_config.bitcoind_rpcuserpass, sizeof(datum_config.bitcoind_rpcuserpass), "%s:%s", datum_config.bitcoind_rpcuser, datum_config.bitcoind_rpcpassword);
	} else if (datum_config.bitcoind_rpccookiefile[0]) {
		update_rpc_cookie(&datum_config);
	} else {
		DLOG_ERROR("Either bitcoind.rpcuser (and bitcoind.rpcpassword) or bitcoind.rpccookiefile is required.");
		return 0;
	}
	
	if (datum_config.stratum_v1_max_threads > MAX_THREADS) {
		DLOG_FATAL("Maximum threads must be less than %d.", MAX_THREADS);
		return 0;
	}
	if (datum_config.stratum_v1_max_clients_per_thread > MAX_CLIENTS_THREAD) {
		DLOG_FATAL("Maximum clients per thread must be less than %d.",MAX_CLIENTS_THREAD);
		return 0;
	}
	if ((strlen(datum_config.mining_coinbase_tag_primary)+strlen(datum_config.mining_coinbase_tag_secondary)) > 88) {
		DLOG_FATAL("Length of coinbase tags can not exceed 88 bytes total.");
		return 0;
	}
	if ((strlen(datum_config.mining_coinbase_tag_primary) > 60) || (strlen(datum_config.mining_coinbase_tag_secondary) > 60)) {
		DLOG_FATAL("Length of coinbase tags can not exceed 88 bytes total or 60 bytes each.");
		return 0;
	}
	if (datum_config.stratum_v1_vardiff_target_shares_min < 1) {
		DLOG_FATAL("Stratum server stratum.vardiff_target_shares_min must be at least 1");
		return 0;
	}
	if (datum_config.stratum_v1_vardiff_quickdiff_count < 4) {
		DLOG_FATAL("Stratum server stratum.vardiff_quickdiff_count must be at least 4");
		return 0;
	}
	if (datum_config.stratum_v1_vardiff_quickdiff_delta < 3) {
		DLOG_FATAL("Stratum server stratum.vardiff_quickdiff_delta must be at least 3");
		return 0;
	}
	if (roundDownToPowerOfTwo_64(datum_config.stratum_v1_vardiff_min) != datum_config.stratum_v1_vardiff_min) {
		const int nv = roundDownToPowerOfTwo_64(datum_config.stratum_v1_vardiff_min);
		DLOG_WARN("stratum.vardiff_min MUST be a power of two. adjusting from %d to %d", datum_config.stratum_v1_vardiff_min, nv);
		datum_config.stratum_v1_vardiff_min = nv;
	}
	if (datum_config.stratum_v1_vardiff_min < 1) {
		DLOG_FATAL("Stratum server stratum.vardiff_min must be at least 1 (suggest at least 1024, but more likely 32768)");
		return 0;
	}
	if (datum_config.stratum_v1_max_clients > (datum_config.stratum_v1_max_clients_per_thread*datum_config.stratum_v1_max_threads)) {
		DLOG_FATAL("Stratum server configuration error. Max clients too high for thread settings");
		return 0;
	}
	if (datum_config.stratum_v1_share_stale_seconds < 60) {
		DLOG_FATAL("Stratum server stratum.share_stale_seconds must be at least 60 (suggest 120)");
		return 0;
	}
	if (datum_config.stratum_v1_share_stale_seconds > 150) {
		DLOG_FATAL("Stratum server stratum.share_stale_seconds must not exceed 150 (suggest 120)");
		return 0;
	}
	return 1;
}

void datum_gateway_help(const char * const argv0) {
	int p;
	const char *lastcat = "";
	static const char * const paddots = "..............................................................";
	
	printf("Usage: %s [OPTION]...\n\n", argv0);
	puts("Command line options:\n");
	puts("    -c, --config=FILE ..................... Path to configuration JSON file (default: ./datum_gateway_config.json)");
	puts("    -?, --help ............................ Print this help");
	puts("    --example-conf ........................ Print an example configuration JSON file");
	puts("    --version ............................. Print this software's name and version");
	puts("");
	puts("Configuration file options:\n\n{");
	for (unsigned int i = 0; i < NUM_CONFIG_ITEMS; ++i) {
		const T_DATUM_CONFIG_ITEM * const opt = &datum_config_options[i];
		if (strcmp(opt->category, lastcat)) {
			if (i) { puts("    },"); }
			printf("    \"%s\": {\n", opt->category);
			lastcat = opt->category;
		}
		p = 30 - strlen(opt->name);
		if (p < 0) p = 0;
		printf("        \"%s\": %.*s %s (%s", opt->name, p, paddots, opt->description, datum_conf_var_type_text[opt->var_type]);
		if (opt->required) {
			puts(", REQUIRED)");
		} else {
			switch (opt->var_type) {
				case DATUM_CONF_INT: {
					printf(", default: %d)\n", opt->default_int);
					break;
				}
				case DATUM_CONF_BOOL: {
					printf(", default: %s)\n", opt->default_bool ? "true" : "false");
					break;
				}
				case DATUM_CONF_STRING: {
					printf(", default: \"%s\")\n", opt->default_string[0]);
					break;
				}
				default: {
					puts(")");
					break;
				}
			}
		}
	}
	puts("    }\n}\n");
}

void datum_gateway_example_conf(void) {
	const char *lastcat = "";
	bool first = true;
	
	puts("{");
	for (unsigned int i = 0; i < NUM_CONFIG_ITEMS; ++i) {
		const T_DATUM_CONFIG_ITEM * const opt = &datum_config_options[i];
		if (!(opt->example || opt->example_default)) {
			continue;
		}
		if (strcmp(opt->category, lastcat)) {
			if (*lastcat) { puts("\n\t},"); }
			printf("\t\"%s\": {\n", opt->category);
			lastcat = opt->category;
			first = true;
		}
		if (first) {
			first = false;
		} else {
			puts(",");
		}
		printf("\t\t\"%s\": ", opt->name);
		if (opt->example) {
			printf("%s", opt->example);
		} else if (opt->example_default) {
			switch (opt->var_type) {
				case DATUM_CONF_INT: {
					printf("%d", opt->default_int);
					break;
				}
				case DATUM_CONF_BOOL: {
					printf("%s", opt->default_bool ? "true" : "false");
					break;
				}
				case DATUM_CONF_STRING: {
					printf("\"%s\"", opt->default_string[0]);
					break;
				}
				case DATUM_CONF_STRING_ARRAY: {
					puts("[]");
					break;
				}
			}
		}
	}
	puts("\n\t}\n}");
}
