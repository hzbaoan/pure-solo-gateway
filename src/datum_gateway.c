/*
 * Pure Solo Gateway
 * Main Application Entry Point
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
#include <argp.h>
#include <signal.h>

#include "datum_gateway.h"
#include "datum_jsonrpc.h"
#include "datum_utils.h"
#include "datum_blocktemplates.h"
#include "datum_stratum.h"
#include "datum_conf.h"
#include "datum_sockets.h"
#include "datum_api.h"

const char *datum_gateway_config_filename = NULL;

// ARGP stuff
const char *argp_program_version = "solo_gateway 1.0.0";
const char *argp_program_bug_address = "<your_email@example.com>";
static char doc[] = "Pure Solo Mining Gateway for Bitcoin";
static char args_doc[] = "";
static struct argp_option options[] = {
	{"help", '?', 0, 0, "Show custom help", 0},
	{"example-conf", 0x100, NULL, 0, "Print an example configuration JSON file", 0},
	{"usage", '?', 0, 0, "Show custom help", 0},
	{"config", 'c', "FILE", 0, "Configuration JSON file"},
	{0}
};

struct arguments {
	char *config_file;
};

static error_t parse_opt(int key, char *arg, struct argp_state *state) {
	struct arguments *arguments = state->input;
	switch (key) {
		case '?': {
			datum_print_banner();
			datum_gateway_help(state->argv[0]);
			exit(0);
			break;
		}
		case 'c': {
			arguments->config_file = arg;
			break;
		}
		case 0x100:  // example-conf
			datum_gateway_example_conf();
			exit(0);
			break;
		default:
			return ARGP_ERR_UNKNOWN;
	}
	return 0;
}

static struct argp argp = {options, parse_opt, args_doc, doc};
// END ARGP Stuff

void datum_print_banner(void) {
	puts("");
	puts(" *****************************************************************");
	puts(" * Pure Solo Gateway --- Optimized for High Performance Mining   *");
	puts(" * Based on OCEAN DATUM Gateway, Refactored for Solo Only        *");
	puts(" *****************************************************************");
	puts("");
	fflush(stdout);
}

const char * const *datum_argv;

int main(const int argc, const char * const * const argv) {
	datum_argv = argv;
	
	struct arguments arguments;
	pthread_t pthread_datum_stratum_v1;
	pthread_t pthread_datum_gateway_template;
	int i;
	
	// Ignore SIGPIPE. This is instead handled gracefully by datum_sockets
	signal(SIGPIPE, SIG_IGN);
	srand(time(NULL)); 
	
	curl_global_init(CURL_GLOBAL_ALL);
	datum_utils_init();
	
	arguments.config_file = "datum_gateway_config.json";  // Default config file
	if (argp_parse(&argp, argc, datum_deepcopy_charpp(argv), 0, 0, &arguments) != 0) {
		datum_print_banner();
		DLOG_FATAL("Error parsing arguments. Check --help");
		exit(1);
	}
	datum_print_banner();
	
	if (datum_read_config(arguments.config_file) != 1) {
		DLOG_FATAL("Error reading config file. Check --help");
		exit(1);
	}
	datum_gateway_config_filename = arguments.config_file;
	
	// Initialize logger thread
	datum_logger_init();
	
#ifdef ENABLE_API
	if (datum_api_init()) {
		DLOG_FATAL("Error initializing API interface");
		usleep(100000);
		exit(1);
	}
#endif
	
	DLOG_DEBUG("Starting template fetcher thread");
	pthread_create(&pthread_datum_gateway_template, NULL, datum_gateway_template_thread, NULL);
	
	DLOG_DEBUG("Starting Stratum v1 server");
	pthread_create(&pthread_datum_stratum_v1, NULL, datum_stratum_v1_socket_server, NULL);
	
	i=0;
	while(1) {
		if (panic_mode) {
			DLOG_FATAL("*** PANIC TRIGGERED: EXITING IMMEDIATELY ***");
			printf("PANIC EXIT.\n");
			sleep(1); 
			fflush(stdout);
			usleep(2000);
			exit(1);
		}
		usleep(500000);
		i++;
		if (i>=600) { // Roughly every 5 minutes spit out some stats to the log
			i = datum_stratum_v1_global_subscriber_count();
			DLOG_INFO("Server stats: %d client%s / %.2f Th/s", i, (i!=1)?"s":"", datum_stratum_v1_est_total_th_sec());
			i=0;
		}
	}
	return 0;
}
