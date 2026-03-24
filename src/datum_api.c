/*
 * Pure Solo Gateway
 * Minimal API and Web Dashboard (API-First Design)
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <microhttpd.h>
#include <jansson.h>

#include "datum_api.h"
#include "datum_conf.h"
#include "datum_gateway.h"
#include "datum_utils.h"
#include "datum_stratum.h"
#include "datum_sockets.h"
#include "web.h"

// -----------------------------------------------------------------------------
// JSON API 数据生成器
// -----------------------------------------------------------------------------
static char* generate_api_stats_json(void) {
	json_t *root = json_object();
	T_DATUM_STRATUM_JOB *sjob = NULL;
	int i, ii;
	int connections = 0;
	int subscriptions = 0;
	double hashrate = datum_stratum_v1_est_total_th_sec();

	// 安全获取当前的任务上下文
	sjob = datum_stratum_latest_job_acquire();

	// 统计矿机连接数
	if (global_stratum_app) {
		for (i = 0; i < global_stratum_app->max_threads; i++) {
			connections += global_stratum_app->datum_threads[i].connected_clients;
			for (ii = 0; ii < global_stratum_app->max_clients_thread; ii++) {
				if (global_stratum_app->datum_threads[i].client_data[ii].fd > 0) {
					T_DATUM_MINER_DATA *m = (T_DATUM_MINER_DATA *)global_stratum_app->datum_threads[i].client_data[ii].app_client_data;
					if (m && m->subscribed) {
						subscriptions++;
					}
				}
			}
		}
	}

	// 组装 JSON
	json_object_set_new(root, "gateway_version", json_string("Pure Solo 1.0"));
	json_object_set_new(root, "uptime_seconds", json_integer(get_process_uptime_seconds()));
	json_object_set_new(root, "hashrate_th_s", json_real(hashrate));
	json_object_set_new(root, "connections", json_integer(connections));
	json_object_set_new(root, "subscriptions", json_integer(subscriptions));
	// 出于隐私保护，移除了 pool_address 的返回

	if (sjob && sjob->block_template) {
		json_object_set_new(root, "current_height", json_integer(sjob->block_template->height));
		json_object_set_new(root, "current_value_btc", json_real((double)sjob->block_template->coinbasevalue / 100000000.0));
		json_object_set_new(root, "txn_count", json_integer(sjob->block_template->txn_count));
		
		// 计算并添加主网当前真实难度
		long double net_diff = calc_network_difficulty(sjob->block_template->bits);
		json_object_set_new(root, "network_difficulty", json_real((double)net_diff));
	} else {
		json_object_set_new(root, "current_height", json_integer(0));
		json_object_set_new(root, "current_value_btc", json_real(0.0));
		json_object_set_new(root, "txn_count", json_integer(0));
		json_object_set_new(root, "network_difficulty", json_real(0.0));
	}

	char *json_str = json_dumps(root, JSON_COMPACT);
	json_decref(root);
	datum_stratum_job_release(sjob);
	return json_str;
}

// -----------------------------------------------------------------------------
// HTTP 服务器路由和处理
// -----------------------------------------------------------------------------
static int send_mhd_response(struct MHD_Connection *connection, const char *data, size_t length, const char *content_type) {
	struct MHD_Response *response = MHD_create_response_from_buffer(length, (void *)data, MHD_RESPMEM_MUST_COPY);
	MHD_add_response_header(response, "Content-Type", content_type);
	MHD_add_response_header(response, "Cache-Control", "no-cache, no-store, must-revalidate");
	int ret = MHD_queue_response(connection, MHD_HTTP_OK, response);
	MHD_destroy_response(response);
	return ret;
}

static enum MHD_Result datum_api_answer(void *cls, struct MHD_Connection *connection, const char *url, const char *method, const char *version, const char *upload_data, size_t *upload_data_size, void **con_cls) {
	if (strcmp(method, "GET") != 0) {
		const char *error_msg = "{\"error\": \"Method not allowed\"}";
		return send_mhd_response(connection, error_msg, strlen(error_msg), "application/json");
	}

	if (strcmp(url, "/api/stats") == 0) {
		char *json_data = generate_api_stats_json();
		enum MHD_Result ret = send_mhd_response(connection, json_data, strlen(json_data), "application/json");
		free(json_data);
		return ret;
	}

	if (strcmp(url, "/api/workers") == 0) {
		char *json_data = datum_stratum_get_workers_json();
		enum MHD_Result ret = send_mhd_response(connection, json_data, strlen(json_data), "application/json");
		free(json_data);
		return ret;
	}

	if (strcmp(url, "/") == 0 || strcmp(url, "/index.html") == 0) {
		const char *html = web_get_html_dashboard();
		return send_mhd_response(connection, html, strlen(html), "text/html");
	}

	const char *not_found = "404 Not Found";
	struct MHD_Response *response = MHD_create_response_from_buffer(strlen(not_found), (void *)not_found, MHD_RESPMEM_PERSISTENT);
	int ret = MHD_queue_response(connection, MHD_HTTP_NOT_FOUND, response);
	MHD_destroy_response(response);
	return ret;
}

static struct MHD_Daemon *datum_api_try_start(unsigned int flags, const int sock) {
	flags |= MHD_USE_AUTO;  
	flags |= MHD_USE_INTERNAL_POLLING_THREAD;
	return MHD_start_daemon(
		flags,
		datum_config.api_listen_port,
		NULL, NULL,  
		&datum_api_answer, NULL,  
		MHD_OPTION_LISTEN_SOCKET, sock,
		MHD_OPTION_CONNECTION_LIMIT, 128,
		MHD_OPTION_LISTENING_ADDRESS_REUSE, (unsigned int)1,
		MHD_OPTION_END
	);
}

void *datum_api_thread(void *ptr) {
	struct MHD_Daemon *daemon;
	
	if (!datum_config.api_listen_port) {
		DLOG_INFO("No API port configured. API disabled.");
		return NULL;
	}
	
	int listen_socks[1];
	size_t listen_socks_len = 1;
	if (!datum_sockets_setup_listening_sockets("API", datum_config.api_listen_addr, datum_config.api_listen_port, listen_socks, &listen_socks_len)) {
		return NULL;
	}
	
	daemon = datum_api_try_start(0, listen_socks[0]);
	
	if (!daemon) {
		DLOG_FATAL("Unable to start daemon for API");
		panic_from_thread(__LINE__);
		return NULL;
	}
	
	DLOG_INFO("Pure Solo Web API Dashboard running on port %d", datum_config.api_listen_port);
	
	while(1) {
		sleep(3);
	}
}

int datum_api_init(void) {
	pthread_t pthread_datum_api_thread;
	
	if (!datum_config.api_listen_port) {
		DLOG_INFO("INFO: API port is 0. Web dashboard disabled.");
		return 0;
	}
	
	int result = pthread_create(&pthread_datum_api_thread, NULL, datum_api_thread, NULL);
	if (result != 0) {
		DLOG_FATAL("Failed to create API thread");
		return -1;
	}
	
	return 0;
}
