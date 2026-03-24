/*
 * Pure Solo Gateway
 * Stratum V1 server
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
#include <sys/resource.h>
#include <math.h>

#include "datum_gateway.h"
#include "datum_stratum.h"
#include "datum_stratum_dupes.h"
#include "datum_jsonrpc.h"
#include "datum_utils.h"
#include "datum_blocktemplates.h"
#include "datum_sockets.h"
#include "datum_conf.h"
#include "datum_coinbaser.h"

T_DATUM_SOCKET_APP *global_stratum_app = NULL;

pthread_rwlock_t stratum_global_job_ptr_lock = PTHREAD_RWLOCK_INITIALIZER;
uint16_t stratum_enprefix = 0;
static T_DATUM_STRATUM_JOB *global_latest_stratum_job = NULL;
static uint64_t global_latest_stratum_job_generation = 0;
static uint64_t global_stratum_block_generation = 0;
static uint64_t stratum_next_job_uid = 1;

static void datum_stratum_job_acquire(T_DATUM_STRATUM_JOB *job) {
	if (!job) return;
	__atomic_add_fetch(&job->refcount, 1, __ATOMIC_RELAXED);
}

void datum_stratum_job_release(T_DATUM_STRATUM_JOB *job) {
	if (!job) return;
	if (__atomic_sub_fetch(&job->refcount, 1, __ATOMIC_ACQ_REL) != 0) return;
	datum_template_release(job->block_template);
	free(job);
}

T_DATUM_STRATUM_JOB *datum_stratum_latest_job_acquire(void) {
	T_DATUM_STRATUM_JOB *job;
	
	pthread_rwlock_rdlock(&stratum_global_job_ptr_lock);
	job = global_latest_stratum_job;
	datum_stratum_job_acquire(job);
	pthread_rwlock_unlock(&stratum_global_job_ptr_lock);
	return job;
}

bool datum_stratum_job_is_stale_prevblock(const T_DATUM_STRATUM_JOB *job) {
	bool stale = true;
	
	if (!job) return true;
	pthread_rwlock_rdlock(&stratum_global_job_ptr_lock);
	stale = (job->block_generation != global_stratum_block_generation);
	pthread_rwlock_unlock(&stratum_global_job_ptr_lock);
	return stale;
}

static void datum_stratum_miner_history_reset(T_DATUM_MINER_DATA *m) {
	uint32_t i;
	
	if (!m) return;
	for(i=0;i<m->job_history_count;i++) {
		datum_stratum_job_release(m->job_history[i].job);
	}
	free(m->job_history);
	m->job_history = NULL;
	m->job_history_count = 0;
	m->job_history_capacity = 0;
	m->next_notify_job_token = 0;
}

static void datum_stratum_miner_history_prune(T_DATUM_MINER_DATA *m, uint64_t now_tsms) {
	uint32_t i, kept = 0;
	const uint64_t max_age_tsms = (uint64_t)datum_config.stratum_v1_share_stale_seconds * (uint64_t)1000;
	
	if (!m || !m->job_history || !m->job_history_count) return;
	for(i=0;i<m->job_history_count;i++) {
		T_DATUM_MINER_JOB_HISTORY_ENTRY *entry = &m->job_history[i];
		if ((now_tsms <= entry->tsms) || ((now_tsms - entry->tsms) <= max_age_tsms)) {
			if (kept != i) {
				m->job_history[kept] = *entry;
			}
			kept++;
			continue;
		}
		datum_stratum_job_release(entry->job);
	}
	m->job_history_count = kept;
}

static bool datum_stratum_miner_history_reserve(T_DATUM_MINER_DATA *m) {
	T_DATUM_MINER_JOB_HISTORY_ENTRY *new_history;
	uint32_t new_capacity;
	
	if (!m) return false;
	if (m->job_history_count < m->job_history_capacity) return true;
	new_capacity = m->job_history_capacity ? (m->job_history_capacity << 1) : 16;
	new_history = realloc(m->job_history, sizeof(T_DATUM_MINER_JOB_HISTORY_ENTRY) * new_capacity);
	if (!new_history) return false;
	m->job_history = new_history;
	m->job_history_capacity = new_capacity;
	return true;
}

static bool datum_stratum_miner_history_record(T_DATUM_MINER_DATA *m, T_DATUM_STRATUM_JOB *job, const char *notify_job_id, const uint8_t *target, uint64_t diff, uint64_t now_tsms) {
	T_DATUM_MINER_JOB_HISTORY_ENTRY *entry;
	
	if (!m || !job || !notify_job_id || !target) return false;
	datum_stratum_miner_history_prune(m, now_tsms);
	if (!datum_stratum_miner_history_reserve(m)) return false;
	entry = &m->job_history[m->job_history_count++];
	memset(entry, 0, sizeof(*entry));
	strncpy(entry->notify_job_id, notify_job_id, sizeof(entry->notify_job_id) - 1);
	entry->job = job;
	entry->diff = diff;
	entry->tsms = now_tsms;
	memcpy(entry->target, target, sizeof(entry->target));
	datum_stratum_job_acquire(job);
	return true;
}

static T_DATUM_MINER_JOB_HISTORY_ENTRY *datum_stratum_miner_history_find(T_DATUM_MINER_DATA *m, const char *notify_job_id, uint64_t now_tsms) {
	int i;
	
	if (!m || !notify_job_id) return NULL;
	datum_stratum_miner_history_prune(m, now_tsms);
	for(i=(int)m->job_history_count-1;i>=0;i--) {
		if (!strcmp(m->job_history[i].notify_job_id, notify_job_id)) {
			return &m->job_history[i];
		}
	}
	return NULL;
}

void *datum_stratum_v1_socket_server(void *arg) {
	T_DATUM_SOCKET_APP *app;
	pthread_t pthread_datum_stratum_socket_server;
	int ret, i, j;
	uint64_t ram_allocated = 0;
	
	DLOG_DEBUG("Stratum V1 server startup");
	app = (T_DATUM_SOCKET_APP *)calloc(1,sizeof(T_DATUM_SOCKET_APP));
	if (!app) {
		panic_from_thread(__LINE__);
		return NULL;
	}
	ram_allocated += sizeof(T_DATUM_SOCKET_APP);
	
	strcpy(app->name, "Solo Stratum V1");
	
	app->init_func = datum_stratum_v1_socket_thread_init;
	app->loop_func = datum_stratum_v1_socket_thread_loop;
	app->client_cmd_func = datum_stratum_v1_socket_thread_client_cmd;
	app->closed_client_func = datum_stratum_v1_socket_thread_client_closed;
	app->new_client_func = datum_stratum_v1_socket_thread_client_new;
	
	app->listen_port = datum_config.stratum_v1_listen_port;
	app->max_clients_thread = datum_config.stratum_v1_max_clients_per_thread;
	app->max_threads = datum_config.stratum_v1_max_threads;
	app->max_clients = datum_config.stratum_v1_max_clients;
	
	app->datum_threads = (T_DATUM_THREAD_DATA *) calloc(app->max_threads + 1, sizeof(T_DATUM_THREAD_DATA));
	ram_allocated += (sizeof(T_DATUM_THREAD_DATA) * (app->max_threads + 1));
	
	app->datum_threads[0].app_thread_data = calloc(app->max_threads + 1, sizeof(T_DATUM_STRATUM_THREADPOOL_DATA));
	for(i=1;i<app->max_threads;i++) {
		app->datum_threads[i].app_thread_data = &((char *)app->datum_threads[0].app_thread_data)[sizeof(T_DATUM_STRATUM_THREADPOOL_DATA)*i];
	}
	ram_allocated += (sizeof(T_DATUM_STRATUM_THREADPOOL_DATA) * (app->max_threads + 1));
	
	app->datum_threads[0].client_data[0].app_client_data = calloc(((app->max_threads*app->max_clients_thread)+1), sizeof(T_DATUM_MINER_DATA));
	ram_allocated += ((app->max_threads*app->max_clients_thread)+1) * sizeof(T_DATUM_MINER_DATA);
	
	for(i=0;i<app->max_threads;i++) {
		for(j=0;j<app->max_clients_thread;j++) {
			if (!((i == 0) && (j == 0))) {
				app->datum_threads[i].client_data[j].app_client_data = &((char *)app->datum_threads[0].client_data[0].app_client_data)[((i*app->max_clients_thread)+j) * sizeof(T_DATUM_MINER_DATA)];
			}
		}
	}
	
	pthread_rwlock_rdlock(&stratum_global_job_ptr_lock);
	i = global_latest_stratum_job_generation ? 1 : 0;
	pthread_rwlock_unlock(&stratum_global_job_ptr_lock);
	
	if (!i) {
		DLOG_DEBUG("Waiting for our first job before starting listening server...");
		j = 0;
		while(!i) {
			usleep(50000);
			pthread_rwlock_rdlock(&stratum_global_job_ptr_lock);
			i = global_latest_stratum_job_generation ? 1 : 0;
			pthread_rwlock_unlock(&stratum_global_job_ptr_lock);
			j++;
		}
	}
	
	DLOG_DEBUG("Starting listener thread %p",app);
	ret = pthread_create(&pthread_datum_stratum_socket_server, NULL, datum_gateway_listener_thread, app);
	if (ret != 0) {
		DLOG_FATAL("Could not pthread_create for socket listener!: %s", strerror(ret));
		panic_from_thread(__LINE__);
		return NULL;
	}
	
	global_stratum_app = app;
	
	while (1) {
		usleep(500000);
	}
	return NULL;
}

int datum_stratum_v1_global_subscriber_count(void) {
	int j, kk, ii;
	T_DATUM_MINER_DATA *m;
	if (!global_stratum_app) return 0;
	kk = 0;
	for(j=0;j<global_stratum_app->max_threads;j++) {
		for(ii=0;ii<global_stratum_app->max_clients_thread;ii++) {
			if (global_stratum_app->datum_threads[j].client_data[ii].fd > 0) {
				m = global_stratum_app->datum_threads[j].client_data[ii].app_client_data;
				if (m->subscribed) kk++;
			}
		}
	}
	return kk;
}

double datum_stratum_v1_est_total_th_sec(void) {
	double hr;
	unsigned char astat;
	double thr = 0.0;
	T_DATUM_MINER_DATA *m = NULL;
	uint64_t tsms;
	int j,ii;
	if (!global_stratum_app) return 0;
	tsms = current_time_millis();
	for(j=0;j<global_stratum_app->max_threads;j++) {
		for(ii=0;ii<global_stratum_app->max_clients_thread;ii++) {
			if (global_stratum_app->datum_threads[j].client_data[ii].fd > 0) {
				m = global_stratum_app->datum_threads[j].client_data[ii].app_client_data;
				if (m->subscribed) {
					astat = m->stats.active_index?0:1; 
					hr = 0.0;
					if ((m->stats.last_swap_ms > 0) && (m->stats.diff_accepted[astat] > 0)) {
						hr = ((double)m->stats.diff_accepted[astat] / (double)((double)m->stats.last_swap_ms/1000.0)) * 0.004294967296; 
					}
					if (((double)(tsms - m->stats.last_swap_tsms)/1000.0) < 180.0) {
						thr += hr;
					}
				}
			}
		}
	}
	return thr;
}

void datum_stratum_v1_socket_thread_client_closed(T_DATUM_CLIENT_DATA *c, const char *msg) {
	T_DATUM_MINER_DATA * const m = c->app_client_data;
	datum_stratum_miner_history_reset(m);
	DLOG_DEBUG("Stratum client connection closed. (%s)", msg);
}

void datum_stratum_v1_socket_thread_client_new(T_DATUM_CLIENT_DATA *c) {
	T_DATUM_MINER_DATA * const m = c->app_client_data;
	DLOG_DEBUG("New Stratum client connected. %d",c->fd);
	datum_stratum_miner_history_reset(m);
	memset(m, 0, sizeof(T_DATUM_MINER_DATA));
	m->sdata = (T_DATUM_STRATUM_THREADPOOL_DATA *)c->datum_thread->app_thread_data;
	m->stats.last_swap_tsms = m->stats.last_share_tsms;
	m->best_share_diff = 0.0;
	if (m->sdata->loop_tsms > 0) {
		m->connect_tsms = m->sdata->loop_tsms;
	} else {
		m->connect_tsms = current_time_millis();
	}
}

void datum_stratum_v1_socket_thread_init(T_DATUM_THREAD_DATA *my) {
	T_DATUM_STRATUM_THREADPOOL_DATA *sdata = (T_DATUM_STRATUM_THREADPOOL_DATA *)my->app_thread_data;
	pthread_rwlock_rdlock(&stratum_global_job_ptr_lock);
	sdata->latest_stratum_job_generation = global_latest_stratum_job_generation;
	sdata->cur_stratum_job = global_latest_stratum_job;
	datum_stratum_job_acquire(sdata->cur_stratum_job);
	pthread_rwlock_unlock(&stratum_global_job_ptr_lock);
	sdata->new_job = false;
	sdata->next_kick_check_tsms = current_time_millis() + 10000;
	datum_stratum_dupes_init(sdata);
}

int datum_stratum_v1_get_thread_subscriber_count(T_DATUM_THREAD_DATA *my) {
	int i,c=0;
	T_DATUM_MINER_DATA *m;
	for(i=0;i<my->app->max_clients_thread;i++) {
		m = my->client_data[i].app_client_data;
		if (my->client_data[i].fd && m->subscribed) {
			c++;
		}
	}
	return c;
}

void datum_stratum_v1_socket_thread_loop(T_DATUM_THREAD_DATA *my) {
	T_DATUM_STRATUM_THREADPOOL_DATA *sdata = (T_DATUM_STRATUM_THREADPOOL_DATA *)my->app_thread_data;
	T_DATUM_STRATUM_JOB *job = NULL;
	T_DATUM_STRATUM_JOB *new_job = NULL;
	T_DATUM_STRATUM_JOB *old_job = NULL;
	T_DATUM_MINER_DATA *m = NULL;
	int i;
	uint64_t latest_generation = 0;
	uint64_t tsms,tsms2,tsms3;
	
	pthread_rwlock_rdlock(&stratum_global_job_ptr_lock);
	if (global_latest_stratum_job_generation != sdata->latest_stratum_job_generation) {
		new_job = global_latest_stratum_job;
		datum_stratum_job_acquire(new_job);
		latest_generation = global_latest_stratum_job_generation;
	}
	pthread_rwlock_unlock(&stratum_global_job_ptr_lock);
	if (new_job) {
		old_job = sdata->cur_stratum_job;
		sdata->cur_stratum_job = new_job;
		sdata->latest_stratum_job_generation = latest_generation;
		sdata->new_job = true;
		sdata->notify_remaining_count = 0;
		datum_stratum_job_release(old_job);
	}
	
	sdata->loop_tsms = current_time_millis();
	job = sdata->cur_stratum_job;
	if (!job) return;
	
	if (sdata->new_job) {
		switch (job->job_state) {
			case JOB_STATE_FULL_PRIORITY: {
				for(i=0;i<my->app->max_clients_thread;i++) {
					m = my->client_data[i].app_client_data;
					if (my->client_data[i].fd && m->subscribed) {
						send_mining_notify(&my->client_data[i],false,false);
					}
				}
				break;
			}
			case JOB_STATE_FULL_NORMAL: {
				sdata->notify_remaining_count = datum_stratum_v1_get_thread_subscriber_count(my);
				if (sdata->notify_remaining_count > 0) {
					sdata->notify_last_cid = -1;
					sdata->notify_start_time = sdata->loop_tsms;
					sdata->notify_delay_per_slot_tsms = ((datum_config.bitcoind_work_update_seconds - 3)*1000) / sdata->notify_remaining_count;
					if (!sdata->notify_delay_per_slot_tsms) sdata->notify_delay_per_slot_tsms = 1;
					sdata->notify_last_time = sdata->loop_tsms - ((sdata->notify_delay_per_slot_tsms / my->app->max_threads) * my->thread_id);
				}
				break;
			}
			default: break;
		}
		sdata->new_job = false;
	}
	
	if (sdata->notify_remaining_count > 0) {
		tsms = sdata->loop_tsms - sdata->notify_last_time;
		if ((!tsms) || (tsms >= sdata->notify_delay_per_slot_tsms)) {
			tsms = tsms / sdata->notify_delay_per_slot_tsms;
			if (!tsms) tsms = 1;
			for(i=(sdata->notify_last_cid+1);i<my->app->max_clients_thread;i++) {
				m = my->client_data[i].app_client_data;
				if (my->client_data[i].fd && m->subscribed && m->subscribe_tsms <= sdata->notify_start_time) {
					send_mining_notify(&my->client_data[i],false,false);
					sdata->notify_remaining_count--;
					sdata->notify_last_cid = i;
					tsms--;
					if (!tsms) break;
				}
			}
			if (i==my->app->max_clients_thread) {
				sdata->notify_remaining_count = 0;
			}
			sdata->notify_last_time = sdata->loop_tsms;
		}
	}
	
	if (sdata->loop_tsms >= sdata->next_kick_check_tsms) {
		if ((datum_config.stratum_v1_idle_timeout_no_subscribe > 0) || (datum_config.stratum_v1_idle_timeout_no_share > 0) || (datum_config.stratum_v1_idle_timeout_max_last_work)) {
			tsms = 1; tsms2 = 1; tsms3 = 1;
			if (datum_config.stratum_v1_idle_timeout_no_subscribe > 0) tsms = sdata->loop_tsms - (datum_config.stratum_v1_idle_timeout_no_subscribe * 1000);
			if (datum_config.stratum_v1_idle_timeout_no_share > 0) tsms2 = sdata->loop_tsms - (datum_config.stratum_v1_idle_timeout_no_share * 1000);
			if (datum_config.stratum_v1_idle_timeout_max_last_work > 0) tsms3 = sdata->loop_tsms - (datum_config.stratum_v1_idle_timeout_max_last_work * 1000);
			
			for(i=0;i<my->app->max_clients_thread;i++) {
				if (my->client_data[i].fd) {
					m = my->client_data[i].app_client_data;
					if (m->subscribed) {
						if (m->share_count_accepted > 0) {
							if (m->stats.last_share_tsms < tsms3) {
								my->client_data[i].kill_request = true;
								my->has_client_kill_request = true;
							}
						} else {
							if (m->connect_tsms < tsms2) {
								my->client_data[i].kill_request = true;
								my->has_client_kill_request = true;
							}
						}
					} else {
						if (m->connect_tsms < tsms) {
							my->client_data[i].kill_request = true;
							my->has_client_kill_request = true;
						}
					}
				}
			}
		}
		sdata->next_kick_check_tsms = sdata->loop_tsms + 11150;
	}
}

void send_error_to_client(T_DATUM_CLIENT_DATA *c, uint64_t id, char *e) {
	char s[1024];
	snprintf(s, sizeof(s), "{\"error\":%s,\"id\":%"PRIu64",\"result\":null}\n", e, id);
	datum_socket_send_string_to_client(c, s);
}

static int stratum_mark_client_send_failed(T_DATUM_CLIENT_DATA *c, const char *message_type, size_t attempted_len) {
	DLOG_WARN("Disconnecting stratum client cid=%d fd=%d after failed %s queue (pending=%d attempted=%zu limit=%d)",
		c ? c->cid : -1,
		c ? c->fd : -1,
		message_type ? message_type : "socket write",
		c ? c->out_buf : -1,
		attempted_len,
		CLIENT_BUFFER);
	if (!c) return -1;
	c->kill_request = true;
	if (c->datum_thread) {
		c->datum_thread->has_client_kill_request = true;
	}
	return -1;
}

static int stratum_queue_bytes_or_kick(T_DATUM_CLIENT_DATA *c, const char *buf, size_t len, const char *message_type) {
	int ret;
	if (!len) return 0;
	ret = datum_socket_send_chars_to_client(c, (char *)buf, (int)len);
	if (ret != (int)len) {
		return stratum_mark_client_send_failed(c, message_type, len);
	}
	return ret;
}

static inline void send_unknown_work_error(T_DATUM_CLIENT_DATA *c, uint64_t id) { send_error_to_client(c, id, "[20,\"unknown-work\",null]"); }
static inline void send_rejected_high_hash_error(T_DATUM_CLIENT_DATA *c, uint64_t id) { send_error_to_client(c, id, "[23,\"high-hash\",null]"); }
static inline void send_rejected_stale(T_DATUM_CLIENT_DATA *c, uint64_t id) { send_error_to_client(c, id, "[21,\"stale-work\",null]"); }
static inline void send_rejected_time_too_old(T_DATUM_CLIENT_DATA *c, uint64_t id) { send_error_to_client(c, id, "[21,\"time-too-old\",null]"); }
static inline void send_rejected_time_too_new(T_DATUM_CLIENT_DATA *c, uint64_t id) { send_error_to_client(c, id, "[21,\"time-too-new\",null]"); }
static inline void send_rejected_stale_block(T_DATUM_CLIENT_DATA *c, uint64_t id) { send_error_to_client(c, id, "[21,\"stale-prevblk\",null]"); }
static inline void send_rejected_hnotzero_error(T_DATUM_CLIENT_DATA *c, uint64_t id) { send_error_to_client(c, id, "[23,\"H-not-zero\",null]"); }
static inline void send_bad_version_error(T_DATUM_CLIENT_DATA *c, uint64_t id) { send_error_to_client(c, id, "[23,\"bad-version\",null]"); }
static inline void send_rejected_duplicate(T_DATUM_CLIENT_DATA *c, uint64_t id) { send_error_to_client(c, id, "[22,\"duplicate\",null]"); }

uint32_t get_new_session_id(T_DATUM_CLIENT_DATA *c) {
	uint32_t i;
	i = ((uint32_t)c->cid) & (uint32_t)0x003FFFFF;
	i |= ((((uint32_t)c->datum_thread->thread_id)<<22) & (uint32_t)0xFFC00000);
	return i ^ 0xB10CF00D;
}

void reset_vardiff_stats(T_DATUM_CLIENT_DATA *c) {
	T_DATUM_MINER_DATA * const m = c->app_client_data;
	m->share_count_since_snap = 0;
	m->share_diff_since_snap = 0;
	m->share_snap_tsms = m->sdata->loop_tsms;
}

void stratum_update_vardiff(T_DATUM_CLIENT_DATA *c, bool no_quick) {
	T_DATUM_MINER_DATA * const m = c->app_client_data;
	uint64_t delta_tsms;
	uint64_t ms_per_share;
	uint64_t target_ms_share;
	
	if (m->current_diff != m->last_sent_diff) return;
	if ((!no_quick) && (m->share_count_since_snap < datum_config.stratum_v1_vardiff_quickdiff_count)) {
		return;
	}
	
	delta_tsms = m->sdata->loop_tsms - m->share_snap_tsms;
	
	if (!m->share_count_since_snap) {
		if (delta_tsms > 60000) {
			m->current_diff = m->current_diff >> 1;
			if (m->current_diff < m->forced_high_min_diff) m->current_diff = m->forced_high_min_diff;
			if (m->current_diff < datum_config.stratum_v1_vardiff_min) m->current_diff = datum_config.stratum_v1_vardiff_min;
			reset_vardiff_stats(c);
		}
		return;
	}
	
	if (delta_tsms < 1000) return;
	
	ms_per_share = delta_tsms / m->share_count_since_snap;
	if (!ms_per_share) ms_per_share = 1;
	target_ms_share = (uint64_t)60000/(uint64_t)datum_config.stratum_v1_vardiff_target_shares_min;
	
	if ((!m->quickdiff_active) && (!no_quick) && (ms_per_share < (target_ms_share/(uint64_t)datum_config.stratum_v1_vardiff_quickdiff_delta))) {
		delta_tsms = roundDownToPowerOfTwo_64((target_ms_share / ms_per_share) * m->current_diff);
		if (delta_tsms < (m->current_diff << 2)) delta_tsms = (m->current_diff << 2);
		m->current_diff = delta_tsms;
		send_mining_notify(c, true, true);
		reset_vardiff_stats(c);
		return;
	}
	
	if (ms_per_share > (target_ms_share*2)) {
		m->current_diff = m->current_diff >> 1;
		if (m->current_diff < m->forced_high_min_diff) m->current_diff = m->forced_high_min_diff;
		if (m->current_diff < datum_config.stratum_v1_vardiff_min) m->current_diff = datum_config.stratum_v1_vardiff_min;
		reset_vardiff_stats(c);
		return;
	}
	
	if (m->share_count_since_snap < 16) return;
	
	if (ms_per_share < (target_ms_share/2)) {
		m->current_diff = m->current_diff << 1;
		reset_vardiff_stats(c);
		return;
	}
	return;
}

#define STAT_CYCLE_MS 60000

void stratum_update_miner_stats_accepted(T_DATUM_CLIENT_DATA *c, uint64_t diff_accepted) {
	T_DATUM_MINER_DATA * const m = c->app_client_data;
	m->stats.diff_accepted[m->stats.active_index?1:0] += diff_accepted;
	m->stats.last_share_tsms = m->sdata->loop_tsms;
	
	if (m->sdata->loop_tsms >= (m->stats.last_swap_tsms+STAT_CYCLE_MS)) {
		m->stats.last_swap_ms = m->sdata->loop_tsms - m->stats.last_swap_tsms;
		m->stats.last_swap_tsms = m->sdata->loop_tsms;
		if (m->stats.active_index) {
			m->stats.active_index = 0;
			m->stats.diff_accepted[0] = 0;
		} else {
			m->stats.active_index = 1;
			m->stats.diff_accepted[1] = 0;
		}
	}
}

// ------------------------------------------------------------
// 通过 256位 Hash 计算真实难度的辅助函数
// ------------------------------------------------------------
static double get_share_actual_diff(const unsigned char *share_hash) {
	double h_val = 0.0;
	int k;
	// share_hash 是以 Little-Endian 存储的 256 位无符号整数
	// 按照算法，将其转换为双精度浮点数
	for (k = 31; k >= 0; k--) {
		h_val = h_val * 256.0 + share_hash[k];
	}
	if (h_val <= 0.0) return 999999999999999.0;
	// 26959946667150639794667015087019630673637144422540572481103610249216.0 
	// 是 Difficulty 1 对应的最大目标值 (0x00000000FFFF000000...)
	return 26959946667150639794667015087019630673637144422540572481103610249216.0 / h_val;
}

int client_mining_submit(T_DATUM_CLIENT_DATA *c, uint64_t id, json_t *params_obj) {
	json_t *job_id, *extranonce2, *ntime, *nonce, *vroll;
	T_DATUM_STRATUM_JOB *job = NULL;
	T_DATUM_MINER_JOB_HISTORY_ENTRY *job_entry = NULL;
	const char *job_id_s, *vroll_s, *extranonce2_s, *ntime_s, *nonce_s;
	uint32_t vroll_uint, bver, ntime_val, nonce_val;
	T_DATUM_STRATUM_COINBASE *cb = NULL;
	unsigned char extranonce_bin[12], block_header[80], digest_temp[40], share_hash[40], full_cb_txn[MAX_COINBASE_TXN_SIZE_BYTES];
	T_DATUM_MINER_DATA * const m = c->app_client_data;
	int i;
	bool quickdiff = false;
	char new_notify_blockhash[65];
	size_t job_id_len;
	uint64_t job_diff;
	uint64_t now_tsms;
	
	job_id = json_array_get(params_obj, 1);
	if (!job_id || !(job_id_s = json_string_value(job_id))) {
		send_unknown_work_error(c,id);
		m->share_count_rejected++; m->share_diff_rejected+=m->last_sent_diff;
		return 0;
	}
	
	job_id_len = strlen(job_id_s);
	if ((job_id_len == 19) && (job_id_s[0] == 'Q')) {
		quickdiff = true;
	} else if (job_id_len != 18) {
		send_unknown_work_error(c,id);
		m->share_count_rejected++; m->share_diff_rejected+=m->last_sent_diff;
		return 0;
	}
	
	now_tsms = m->sdata->loop_tsms ? m->sdata->loop_tsms : current_time_millis();
	job_entry = datum_stratum_miner_history_find(m, job_id_s, now_tsms);
	if (!job_entry) {
		send_unknown_work_error(c,id);
		m->share_count_rejected++; m->share_diff_rejected+=m->last_sent_diff;
		return 0;
	}
	
	job = job_entry->job;
	job_diff = job_entry->diff;
	bver = job->version_uint;
	
	if (m->extension_version_rolling) {
		vroll = json_array_get(params_obj, 5);
		if (!vroll || !(vroll_s = json_string_value(vroll))) {
			send_bad_version_error(c,id);
			m->share_count_rejected++; m->share_diff_rejected += job_diff;
			return 0;
		}
		vroll_uint = strtoul(vroll_s, NULL, 16);
		if ((vroll_uint & m->extension_version_rolling_mask) != vroll_uint) {
			send_bad_version_error(c,id);
			m->share_count_rejected++; m->share_diff_rejected += job_diff;
			return 0;
		}
		bver |= vroll_uint;
	}
	
	pk_u32le(block_header, 0, bver);
	memcpy(&block_header[4], job->prevhash_bin, 32);
	pk_u32le(extranonce_bin, 0, m->sid_inv);
	
	extranonce2 = json_array_get(params_obj, 2);
	if (!extranonce2 || !(extranonce2_s = json_string_value(extranonce2)) || strlen(extranonce2_s) != 16) {
		send_unknown_work_error(c, id);
		m->share_count_rejected++; m->share_diff_rejected += job_diff;
		return 0;
	}
	for(i=0;i<8;i++) extranonce_bin[i+4] = hex2bin_uchar(&extranonce2_s[i<<1]);
	
	cb = &job->coinbase;
	
	memcpy(&full_cb_txn[0], cb->coinb1_bin, cb->coinb1_len);
	memcpy(&full_cb_txn[cb->coinb1_len], extranonce_bin, 12);
	memcpy(&full_cb_txn[cb->coinb1_len+12], cb->coinb2_bin, cb->coinb2_len);
	
	if (quickdiff) {
		if (upk_u16le(full_cb_txn, cb->coinb1_len - 2) != 0x5144) pk_u16le(full_cb_txn, cb->coinb1_len - 2, 0x5144);
		else pk_u16le(full_cb_txn, cb->coinb1_len - 2, 0xAEBB);
		full_cb_txn[job->target_pot_index] = floorPoT(job_diff);
	} else {
		full_cb_txn[job->target_pot_index] = floorPoT(job_diff);
	}
	
	double_sha256(digest_temp, full_cb_txn, cb->coinb1_len+12+cb->coinb2_len);
	stratum_job_merkle_root_calc(job, digest_temp, &block_header[36]);
	
	ntime = json_array_get(params_obj, 3);
	if (!ntime || !(ntime_s = json_string_value(ntime))) {
		send_unknown_work_error(c, id);
		m->share_count_rejected++; m->share_diff_rejected += job_diff;
		return 0;
	}
	ntime_val = strtoul(ntime_s, NULL, 16);
	pk_u32le(block_header, 68, ntime_val);
	memcpy(&block_header[72], &job->nbits_bin[0], sizeof(uint32_t));
	
	nonce = json_array_get(params_obj, 4);
	if (!nonce || !(nonce_s = json_string_value(nonce))) {
		send_unknown_work_error(c, id);
		m->share_count_rejected++; m->share_diff_rejected += job_diff;
		return 0;
	}
	nonce_val = strtoul(nonce_s, NULL, 16);
	pk_u32le(block_header, 76, nonce_val);
	
	my_sha256(digest_temp, block_header, 80);
	my_sha256(share_hash, digest_temp, 32);
	
	if (upk_u32le(share_hash, 28) != 0) {
		send_rejected_hnotzero_error(c, id);
		m->share_count_rejected++; m->share_diff_rejected += job_diff;
		return 0;
	}
	
	// Check if block found!
	if (compare_hashes(share_hash, job->block_target) <= 0) {
		new_notify_blockhash[64] = 0;
		for(i=0;i<32;i++) uchar_to_hex((char *)&new_notify_blockhash[(31-i)<<1], share_hash[i]);
		
		DLOG_WARN("************************************************************************************************");
		DLOG_WARN("******** SOLO BLOCK FOUND - %s ********",new_notify_blockhash);
		DLOG_WARN("************************************************************************************************");
		
		i = assembleBlockAndSubmit(block_header, full_cb_txn, cb->coinb1_len+12+cb->coinb2_len, job, m->sdata, new_notify_blockhash);
		if (i) {
			datum_blocktemplates_notifynew(new_notify_blockhash, job->height + 1);
		}
	}
	
	if (datum_stratum_job_is_stale_prevblock(job)) {
		send_rejected_stale_block(c, id);
		m->share_count_rejected++; m->share_diff_rejected += job_diff;
		return 0;
	}
	
	if (ntime_val < job->block_template->mintime) {
		send_rejected_time_too_old(c, id);
		m->share_count_rejected++; m->share_diff_rejected += job_diff;
		return 0;
	}
	
	if (ntime_val > (job->block_template->curtime + 7200)) {
		send_rejected_time_too_new(c, id);
		m->share_count_rejected++; m->share_diff_rejected += job_diff;
		return 0;
	}
	
	if (compare_hashes(share_hash, job_entry->target) > 0) {
		send_rejected_high_hash_error(c, id);
		m->share_count_rejected++; m->share_diff_rejected += job_diff;
		return 0;
	}
	
	if (now_tsms > (job->tsms + ((datum_config.stratum_v1_share_stale_seconds + datum_config.bitcoind_work_update_seconds) * 1000))) {
		send_rejected_stale(c, id);
		m->share_count_rejected++; m->share_diff_rejected += job_diff;
		return 0;
	}
	
	if (datum_stratum_check_for_dupe(m->sdata, nonce_val, job->job_uid, quickdiff?(~ntime_val):(ntime_val), bver, &extranonce_bin[0])) {
		send_rejected_duplicate(c, id);
		m->share_count_rejected++; m->share_diff_rejected += job_diff;
		return 0;
	}
	
	// ---> 这里修改为计算实际难度并比较更新 <---
	double actual_diff = get_share_actual_diff(share_hash);
	if (actual_diff > m->best_share_diff) {
		m->best_share_diff = actual_diff;
	}
	
	// work accepted (Locally, for vardiff and stats. No external forwarding!)
	char s[256];
	snprintf(s, sizeof(s), "{\"error\":null,\"id\":%"PRIu64",\"result\":true}\n", id);
	datum_socket_send_string_to_client(c, s);
	
	m->share_diff_accepted += job_diff;
	m->share_count_accepted++;
	m->share_count_since_snap++;
	m->share_diff_since_snap += job_diff;
	
	stratum_update_miner_stats_accepted(c, job_diff);
	stratum_update_vardiff(c,false);
	
	return 0;
}

int client_mining_configure(T_DATUM_CLIENT_DATA *c, uint64_t id, json_t *params_obj) {
	json_t *p1, *p2, *t;
	const char *s, *s2;
	char sx[1024]; char sa[1024];
	int sxl = 0, i;
	sx[0] = 0;
	T_DATUM_MINER_DATA * const m = c->app_client_data;
	bool new_vroll = false, new_mdiff = false;
	bool wrote_result = false;
	
	if (!json_is_array(params_obj)) return -1;
	p1 = json_array_get(params_obj, 0); p2 = json_array_get(params_obj, 1);
	if ((!p1) || (!p2)) return -1;
	
	size_t index; json_t *value;
	json_array_foreach(p1, index, value) {
		if (json_is_string(value)) {
			s = json_string_value(value);
			switch(s[0]) {
				case 'v': {
					if (!strcmp("version-rolling", s)) {
						new_vroll = true; m->extension_version_rolling = true;
						m->extension_version_rolling_mask = 0x1fffe000;
						t = json_object_get(p2, "version-rolling.mask");
						if (t && (s2 = json_string_value(t))) m->extension_version_rolling_mask = strtoul(s2, NULL, 16) & m->extension_version_rolling_mask;
						sxl = sprintf(&sx[sxl], "{\"id\":null,\"method\":\"mining.set_version_mask\",\"params\":[\"%08x\"]}\n", m->extension_version_rolling_mask);
					}
					break;
				}
				case 'm': {
					if (!strcmp("minimum-difficulty", s)) new_mdiff = true;
					break;
				}
				default: break;
			}
		}
	}
	
	i = snprintf(sa, sizeof(sa), "{\"error\":null,\"id\":%"PRIu64",\"result\":{", id);
	if (new_vroll) {
		i += snprintf(&sa[i], sizeof(sa)-i, "\"version-rolling\":true,\"version-rolling.mask\":\"%08x\"", m->extension_version_rolling_mask);
		wrote_result = true;
	}
	if (new_mdiff) {
		i += snprintf(&sa[i], sizeof(sa)-i, "%s\"minimum-difficulty\":false", wrote_result ? "," : "");
		wrote_result = true;
	}
	i+= snprintf(&sa[i], sizeof(sa)-i, "}}\n");
	
	datum_socket_send_string_to_client(c, sa);
	if (sxl) datum_socket_send_string_to_client(c, sx);
	return 0;
}

int client_mining_authorize(T_DATUM_CLIENT_DATA *c, uint64_t id, json_t *params_obj) {
	char s[256];
	const char *username_s;
	json_t *username;
	T_DATUM_MINER_DATA * const m = c->app_client_data;
	
	username = json_array_get(params_obj, 0);
	username_s = (username && json_string_value(username)) ? json_string_value(username) : "NULL";
	
	strncpy(m->last_auth_username, username_s, sizeof(m->last_auth_username) - 1);
	m->last_auth_username[sizeof(m->last_auth_username)-1] = 0;
	snprintf(s, sizeof(s), "{\"error\":null,\"id\":%"PRIu64",\"result\":true}\n", id);
	datum_socket_send_string_to_client(c, s);
	return 0;
}

int send_mining_notify(T_DATUM_CLIENT_DATA *c, bool clean, bool quickdiff) {
	T_DATUM_THREAD_DATA *t = (T_DATUM_THREAD_DATA *)c->datum_thread;
	T_DATUM_STRATUM_JOB *j = ((T_DATUM_STRATUM_THREADPOOL_DATA *)t->app_thread_data)->cur_stratum_job;
	T_DATUM_MINER_DATA * const m = c->app_client_data;
	T_DATUM_STRATUM_COINBASE *cb;
	char cb1[STRATUM_COINBASE1_MAX_LEN+2];
	char notify_job_id[24];
	char notify_msg[CLIENT_BUFFER];
	char s[512];
	size_t notify_len = 0;
	size_t cb1_hex_len = 0;
	const char *clean_flag;
	unsigned char tdiff = 0xFF;
	unsigned char share_target[32];
	uint64_t job_diff;
	int n;
	int ret;
	
	if (!j) return -1;
	clean_flag = ((clean) || j->clean || (quickdiff)) ? "true" : "false";
	
	if (!quickdiff) stratum_update_vardiff(c, true);
	
	if (m->last_sent_diff != m->current_diff) {
		if (send_mining_set_difficulty(c) < 0) return -1;
	}
	
	job_diff = m->last_sent_diff;
	get_target_from_diff(share_target, job_diff);
	m->quickdiff_active = quickdiff;
	
	cb = &j->coinbase;
	if (quickdiff) {
		n = snprintf(notify_job_id, sizeof(notify_job_id), "Q%s%4.4x", j->job_id, (unsigned int)m->next_notify_job_token++);
	} else {
		n = snprintf(notify_job_id, sizeof(notify_job_id), "%s%4.4x", j->job_id, (unsigned int)m->next_notify_job_token++);
	}
	if (n < 0 || (size_t)n >= sizeof(notify_job_id)) {
		return stratum_mark_client_send_failed(c, "mining.notify", 0);
	}
	
	n = snprintf(s, sizeof(s), "\"%s\",\"%s\",\"", notify_job_id, j->prevhash);
	if (n < 0 || (size_t)n >= sizeof(s)) {
		return stratum_mark_client_send_failed(c, "mining.notify", 0);
	}
	
	n = snprintf(notify_msg, sizeof(notify_msg), "{\"id\":null,\"method\":\"mining.notify\",\"params\":[%s", s);
	if (n < 0 || (size_t)n >= sizeof(notify_msg)) {
		return stratum_mark_client_send_failed(c, "mining.notify", 0);
	}
	notify_len = (size_t)n;
	memcpy(cb1, cb->coinb1, cb->coinb1_len<<1);
	cb1[cb->coinb1_len<<1] = 0;
	cb1_hex_len = (size_t)(cb->coinb1_len << 1);
	
	tdiff = floorPoT(job_diff);
	uchar_to_hex(&cb1[j->target_pot_index<<1], tdiff);
	
	if (quickdiff) {
		const size_t quickdiff_cb1_len = cb1_hex_len - 4;
		const char *quickdiff_tail = (upk_u16le(cb->coinb1_bin, cb->coinb1_len - 2) != 0x5144) ? "4451" : "BBAE";
		if (quickdiff_cb1_len >= (sizeof(notify_msg) - notify_len)) {
			return stratum_mark_client_send_failed(c, "mining.notify", notify_len + quickdiff_cb1_len);
		}
		memcpy(&notify_msg[notify_len], cb1, quickdiff_cb1_len);
		notify_len += quickdiff_cb1_len;
		notify_msg[notify_len] = 0;
		n = snprintf(&notify_msg[notify_len], sizeof(notify_msg) - notify_len, "%s\",\"%s\",", quickdiff_tail, cb->coinb2);
	} else {
		n = snprintf(&notify_msg[notify_len], sizeof(notify_msg) - notify_len, "%s\",\"%s\",", cb1, cb->coinb2);
	}
	if (n < 0 || (size_t)n >= (sizeof(notify_msg) - notify_len)) {
		return stratum_mark_client_send_failed(c, "mining.notify", notify_len);
	}
	notify_len += (size_t)n;
	
	n = snprintf(&notify_msg[notify_len], sizeof(notify_msg) - notify_len, "%s", j->merklebranches_full);
	if (n < 0 || (size_t)n >= (sizeof(notify_msg) - notify_len)) {
		return stratum_mark_client_send_failed(c, "mining.notify", notify_len);
	}
	notify_len += (size_t)n;
	
	n = snprintf(&notify_msg[notify_len], sizeof(notify_msg) - notify_len, ",\"%s\",\"%s\",\"%s\",%s]}\n", j->version, j->nbits, j->ntime, clean_flag);
	if (n < 0 || (size_t)n >= (sizeof(notify_msg) - notify_len)) {
		return stratum_mark_client_send_failed(c, "mining.notify", notify_len);
	}
	notify_len += (size_t)n;
	
	ret = stratum_queue_bytes_or_kick(c, notify_msg, notify_len, "mining.notify");
	if (ret < 0) return ret;
	if (!datum_stratum_miner_history_record(m, j, notify_job_id, share_target, job_diff, m->sdata->loop_tsms ? m->sdata->loop_tsms : current_time_millis())) {
		return stratum_mark_client_send_failed(c, "mining.notify", notify_len);
	}
	return ret;
}

int send_mining_set_difficulty(T_DATUM_CLIENT_DATA *c) {
	char s[256];
	T_DATUM_MINER_DATA * const m = c->app_client_data;
	int len;
	if (!m->current_diff) m->current_diff = datum_config.stratum_v1_vardiff_min;
	len = snprintf(s, sizeof(s), "{\"id\":null,\"method\":\"mining.set_difficulty\",\"params\":[%"PRIu64"]}\n", (uint64_t)m->current_diff);
	if (len < 0 || (size_t)len >= sizeof(s)) {
		return stratum_mark_client_send_failed(c, "mining.set_difficulty", 0);
	}
	if (stratum_queue_bytes_or_kick(c, s, (size_t)len, "mining.set_difficulty") < 0) {
		return -1;
	}
	m->last_sent_diff = m->current_diff;
	return 0;
}

void datum_stratum_fingerprint_by_UA(T_DATUM_MINER_DATA *m) {
	if (strstr(m->useragent, "NiceHash/") == m->useragent) {
		m->current_diff = 524288; 
		m->forced_high_min_diff = 524288; 
		return;
	}
}

int client_mining_subscribe(T_DATUM_CLIENT_DATA *c, uint64_t id, json_t *params_obj) {
	uint32_t sid; char s[1024];
	T_DATUM_MINER_DATA * const m = c->app_client_data;
	json_t *useragent;
	
	if (m->subscribed) return 0;
	m->current_diff = datum_config.stratum_v1_vardiff_min;
	m->useragent[0] = 0;
	
	if (params_obj && json_is_array(params_obj)) {
		useragent = json_array_get(params_obj, 0);
		if (json_is_string(useragent)) {
			strncpy_uachars(m->useragent, json_string_value(useragent), 127);
			m->useragent[127] = 0;
		}
	}
	
	if ((datum_config.stratum_v1_fingerprint_miners) && (m->useragent[0])) {
		datum_stratum_fingerprint_by_UA(m);
		if (m->current_diff < datum_config.stratum_v1_vardiff_min) m->current_diff = datum_config.stratum_v1_vardiff_min;
	}
	
	sid = get_new_session_id(c);
	m->sid = sid;
	m->sid_inv = ((sid>>24)&0xff) | (((sid>>16)&0xff)<<8) | (((sid>>8)&0xff)<<16) | ((sid&0xff)<<24);
	
	snprintf(s, sizeof(s), "{\"error\":null,\"id\":%"PRIu64",\"result\":[[[\"mining.notify\",\"%8.8x1\"],[\"mining.set_difficulty\",\"%8.8x2\"]],\"%8.8x\",8]}\n", id, sid, sid, sid);
	datum_socket_send_string_to_client(c, s);
	send_mining_set_difficulty(c);
	m->subscribed = true;
	send_mining_notify(c, true, false);
	
	m->share_count_since_snap = 0; m->share_diff_since_snap = 0;
	m->share_snap_tsms = m->sdata->loop_tsms; m->subscribe_tsms = m->sdata->loop_tsms;
	return 0;
}

int datum_stratum_v1_socket_thread_client_cmd(T_DATUM_CLIENT_DATA *c, char *line) {
	json_t *j ,*method_obj, *id_obj, *params_obj;
	json_error_t err = { };
	const char *method; int i; uint64_t id;
	
	if (line[0] == 0) return 0;
	if (line[0] != '{') return -1;
	
	j = json_loads(line, JSON_REJECT_DUPLICATES, &err);
	if (!j) return -2;
	if (!(method_obj = json_object_get(j, "method"))) { json_decref(j); return -3; }
	if (!json_is_string(method_obj)) { json_decref(j); return -6; }
	if (!(id_obj = json_object_get(j, "id")) || !json_is_integer(id_obj)) { json_decref(j); return -4; }
	id = json_integer_value(id_obj);
	if (!(params_obj = json_object_get(j, "params"))) { json_decref(j); return -5; }
	
	method = json_string_value(method_obj);
	if (method[0] == 0) { json_decref(j); return -7; }
	
	switch (method[0]) {
		case 'm': {
			if (!strcmp(method, "mining.submit")) { i = client_mining_submit(c, id, params_obj); json_decref(j); return i; }
			if (!strcmp(method, "mining.configure")) { i = client_mining_configure(c, id, params_obj); json_decref(j); return i; }
			if (!strcmp(method, "mining.subscribe")) { i = client_mining_subscribe(c, id, params_obj); json_decref(j); return i; }
			if (!strcmp(method, "mining.authorize")) { i = client_mining_authorize(c, id, params_obj); json_decref(j); return i; }
			[[fallthrough]];
		}
		default: {
			send_error_to_client(c, id, "[-3,\"Method not found\",null]");
			json_decref(j); return 0;
		}
	}
}

void stratum_job_merkle_root_calc(T_DATUM_STRATUM_JOB *s, unsigned char *coinbase_txn_hash, unsigned char *merkle_root_output) {
	int i; unsigned char combined[64]; unsigned char next[32];
	if (!s->merklebranch_count) { memcpy(merkle_root_output, coinbase_txn_hash, 32); return; }
	
	memcpy(&combined[0], coinbase_txn_hash, 32); memcpy(&combined[32], s->merklebranches_bin[0], 32);
	double_sha256(next, combined, 64);
	for(i=1;i<s->merklebranch_count;i++) {
		memcpy(&combined[0], next, 32); memcpy(&combined[32], s->merklebranches_bin[i], 32);
		double_sha256(next, combined, 64);
	}
	memcpy(merkle_root_output, next, 32);
	return;
}

void stratum_calculate_merkle_branches(T_DATUM_STRATUM_JOB *s) {
	bool level_needs_dupe = false; int current_level_size = 0, next_level_size = 0, q, i, j;
	unsigned char combined[64];
	unsigned char (*current_level)[32];
	unsigned char (*next_level)[32];
	unsigned char (*swap_level)[32];
	// txn_count is capped at 16383, so coinbase + txns yields at most 16384 leaves.
	static unsigned char level_a[16384][32];
	static unsigned char level_b[16384][32];
	static int safety_check; int marker = ++safety_check;
	
	if (s->block_template->txn_count > 16383) {
		panic_from_thread(__LINE__); return;
	}
	if (!s->block_template->txn_count) {
		s->merklebranch_count = 0; s->merklebranches_full[0] = '['; s->merklebranches_full[1] = ']'; s->merklebranches_full[2] = 0;
		return;
	}
	
	current_level_size = s->block_template->txn_count+1;
	current_level = level_a;
	next_level = level_b;
	level_needs_dupe = false; q = 0;
	
	while (current_level_size > 1) {
		if (current_level_size % 2 != 0) { current_level_size++; level_needs_dupe = true; }
		else level_needs_dupe = false;
		
		next_level_size = current_level_size >> 1;
		for(i=0;i<next_level_size;i++) {
			if (!i) {
				if (!q) {
					memcpy(s->merklebranches_bin[0], s->block_template->txns[0].txid_bin, 32);
					for(j=0;j<32;j++) pk_u16le(s->merklebranches_hex[0], j << 1, upk_u16le(s->block_template->txns[0].txid_hex, (31 - j) << 1));
					s->merklebranches_hex[0][64] = 0;
				} else {
					if (level_needs_dupe && (i==(next_level_size-1))) memcpy(s->merklebranches_bin[q], &current_level[i<<1][0], 32);
					else memcpy(s->merklebranches_bin[q], &current_level[(i<<1)+1][0], 32);
					hash2hex(s->merklebranches_bin[q], s->merklebranches_hex[q]);
				}
			} else {
				if (!q) {
					memcpy(&combined[0], s->block_template->txns[(i<<1)-1].txid_bin, 32);
					if (level_needs_dupe && (i==(next_level_size-1))) memcpy(&combined[32], s->block_template->txns[(i<<1)-1].txid_bin, 32);
					else memcpy(&combined[32], s->block_template->txns[(i<<1)].txid_bin, 32);
				} else {
					memcpy(&combined[0], &current_level[i<<1][0], 32);
					if (level_needs_dupe && (i==(next_level_size-1))) memcpy(&combined[32], &current_level[i<<1][0], 32);
					else memcpy(&combined[32], &current_level[(i<<1)+1][0], 32);
				}
				double_sha256(next_level[i], combined, 64);
			}
		}
		swap_level = current_level;
		current_level = next_level;
		next_level = swap_level;
		current_level_size = next_level_size; q++;
	}
	s->merklebranch_count = q;
	
	s->merklebranches_full[0] = '['; j=1;
	for(i=0;i<q;i++) {
		if (i) { s->merklebranches_full[j] = ','; j++; }
		j += sprintf(&s->merklebranches_full[j], "\"%s\"", s->merklebranches_hex[i]);
	}
	s->merklebranches_full[j] = ']'; s->merklebranches_full[j+1] = 0;
	if (safety_check != marker) panic_from_thread(__LINE__);
}

void update_stratum_job(T_DATUM_TEMPLATE_DATA *block_template, bool new_block, bool clean, int job_state) {
	T_DATUM_STRATUM_JOB *s;
	T_DATUM_STRATUM_JOB *old_job;
	uint64_t publish_generation;
	uint64_t block_generation;
	int i;
	
	s = calloc(1, sizeof(T_DATUM_STRATUM_JOB));
	if (!s) {
		panic_from_thread(__LINE__);
		return;
	}
	s->refcount = 1;
	datum_template_acquire(block_template);
	
	s->enprefix = stratum_enprefix ^ 0xB10C; stratum_enprefix++;
	for(i=0;i<8;i++) pk_u64le(s->prevhash, i << 3, upk_u64le(block_template->previousblockhash, (7 - i) << 3));
	s->prevhash[64] = 0;
	
	snprintf(s->version, sizeof(s->version), "%8.8x", block_template->version);
	s->version_uint = block_template->version;
	strncpy(s->nbits, block_template->bits, sizeof(s->nbits) - 1);
	snprintf(s->ntime, sizeof(s->ntime), "%8.8llx", (unsigned long long)block_template->curtime);
	
	s->coinbase_value = block_template->coinbasevalue;
	s->height = block_template->height; s->block_template = block_template;
	memcpy(s->prevhash_bin, block_template->previousblockhash_bin, 32);
	memcpy(s->nbits_bin, block_template->bits_bin, 4);
	s->nbits_uint = upk_u32le(s->nbits_bin, 0);
	nbits_to_target(s->nbits_uint, s->block_target);
	
	s->clean = clean;
	
	generate_base_coinbase_txns_for_stratum_job(s);
	s->job_state = job_state;
	s->tsms = current_time_millis();
	stratum_calculate_merkle_branches(s);
	
	pthread_rwlock_wrlock(&stratum_global_job_ptr_lock);
	if (new_block) global_stratum_block_generation++;
	block_generation = global_stratum_block_generation;
	publish_generation = stratum_next_job_uid++;
	s->job_uid = publish_generation;
	s->publish_generation = publish_generation;
	s->block_generation = block_generation;
	s->global_index = (int)(publish_generation & 0x7FFFFFFF);
	snprintf(s->job_id, sizeof(s->job_id), "%014" PRIx64, publish_generation & UINT64_C(0x00FFFFFFFFFFFFFF));
	old_job = global_latest_stratum_job;
	global_latest_stratum_job = s;
	global_latest_stratum_job_generation = publish_generation;
	pthread_rwlock_unlock(&stratum_global_job_ptr_lock);
	datum_stratum_job_release(old_job);
	DLOG_DEBUG("Updated to job %d, state = %d", s->global_index, s->job_state);
	return;
}

static size_t build_submit_coinbase_txn(uint8_t *dst, size_t dst_size, const uint8_t *stripped_coinbase_txn, size_t stripped_coinbase_txn_size) {
	size_t offset = 0;
	
	// submitblock expects the fully serialized segwit coinbase tx, while miners hash the stripped form for txid/merkle work.
	if (!dst || !stripped_coinbase_txn || stripped_coinbase_txn_size < 8) return 0;
	if (dst_size < (stripped_coinbase_txn_size + 36)) return 0;
	
	memcpy(&dst[offset], stripped_coinbase_txn, 4);
	offset += 4;
	dst[offset++] = 0x00;
	dst[offset++] = 0x01;
	memcpy(&dst[offset], &stripped_coinbase_txn[4], stripped_coinbase_txn_size - 8);
	offset += stripped_coinbase_txn_size - 8;
	dst[offset++] = 0x01;
	dst[offset++] = 0x20;
	memset(&dst[offset], 0, 32);
	offset += 32;
	memcpy(&dst[offset], &stripped_coinbase_txn[stripped_coinbase_txn_size - 4], 4);
	offset += 4;
	
	return offset;
}

static bool submitblock_result_is_duplicate(const json_t *reply) {
	const json_t *result;
	const char *result_s;
	
	if (!reply) return false;
	result = json_object_get(reply, "result");
	if (!json_is_string(result)) return false;
	result_s = json_string_value(result);
	return result_s && (!strcmp(result_s, "duplicate") || !strcmp(result_s, "duplicate-inconclusive"));
}

static void preciousblock_local(CURL *tcurl, const char *block_hash_hex) {
	char rpc_data[384];
	json_t *reply;
	
	snprintf(rpc_data, sizeof(rpc_data), "{\"method\":\"preciousblock\",\"params\":[\"%s\"],\"id\":1}", block_hash_hex);
	reply = bitcoind_json_rpc_call_allow_null_result(tcurl, &datum_config, rpc_data);
	if (reply) json_decref(reply);
}

static bool submitblock_rpc(CURL *tcurl, const char *url, const char *submitblock_req, const char *block_hash_hex) {
	json_t *reply;
	char *reply_dump = NULL;
	const char *target = url ? url : datum_config.bitcoind_rpcurl;
	bool accepted = false;
	
	if (url) reply = json_rpc_call_allow_null_result(tcurl, url, NULL, submitblock_req);
	else reply = bitcoind_json_rpc_call_allow_null_result(tcurl, &datum_config, submitblock_req);
	
	if (!reply) {
		DLOG_ERROR("Could not confirm submitblock RPC result for block %s on %s", block_hash_hex, target);
		return false;
	}
	
	if (json_is_null(json_object_get(reply, "result"))) {
		DLOG_INFO("Block %s submitted to %s successfully", block_hash_hex, target);
		accepted = true;
	} else if (submitblock_result_is_duplicate(reply)) {
		DLOG_WARN("Node %s already knows block %s", target, block_hash_hex);
		accepted = true;
	} else {
		reply_dump = json_dumps(reply, JSON_ENCODE_ANY);
		if (reply_dump) {
			DLOG_WARN("Node %s rejected block %s (%s)", target, block_hash_hex, reply_dump);
			free(reply_dump);
		} else {
			DLOG_WARN("Node %s rejected block %s", target, block_hash_hex);
		}
	}
	
	json_decref(reply);
	return accepted;
}

int assembleBlockAndSubmit(uint8_t *block_header, uint8_t *coinbase_txn, size_t coinbase_txn_size, T_DATUM_STRATUM_JOB *job, T_DATUM_STRATUM_THREADPOOL_DATA *sdata, const char *block_hash_hex) {
	char *submitblock_req = NULL, *ptr = NULL;
	size_t i;
	CURL *tcurl;
	int ret = 0;
	bool free_submitblock_req = false;
	uint8_t submit_coinbase_txn[MAX_COINBASE_TXN_SIZE_BYTES];
	const uint8_t *submit_coinbase_txn_ptr = coinbase_txn;
	size_t submit_coinbase_txn_size = coinbase_txn_size;
	
	submitblock_req = sdata->submitblock_req;
	if (!submitblock_req) {
		submitblock_req = malloc(8500000); 
		if (!submitblock_req) { panic_from_thread(__LINE__); return 0; }
		free_submitblock_req = true;
	}
	submit_coinbase_txn_size = build_submit_coinbase_txn(submit_coinbase_txn, sizeof(submit_coinbase_txn), coinbase_txn, coinbase_txn_size);
	if (!submit_coinbase_txn_size) {
		DLOG_ERROR("Could not build full submit coinbase for block %s", block_hash_hex);
		if (free_submitblock_req) free(submitblock_req);
		return 0;
	}
	submit_coinbase_txn_ptr = submit_coinbase_txn;
	
	ptr = submitblock_req;
	ptr += sprintf(ptr, "{\"jsonrpc\":\"1.0\",\"id\":\"%llu\",\"method\":\"submitblock\",\"params\":[\"",(unsigned long long)time(NULL));
	for(i=0;i<80;i++) ptr += sprintf(ptr, "%2.2x", block_header[i]);
	
	ptr += append_bitcoin_varint_hex(job->block_template->txn_count + 1, ptr);
	
	for(i=0;i<submit_coinbase_txn_size;i++) ptr += sprintf(ptr, "%2.2x", submit_coinbase_txn_ptr[i]);
	
	for(i=0;i<job->block_template->txn_count;i++) {
		memcpy(ptr, job->block_template->txns[i].txn_data_hex, job->block_template->txns[i].size*2);
		ptr += job->block_template->txns[i].size*2;
	}
	
	*ptr = '"'; ptr++; *ptr = ']'; ptr++; *ptr = '}'; ptr++; *ptr = 0;
	
	if (datum_config.mining_save_submitblocks_dir[0] != 0) {
		char submitblockpath[384];
		if (snprintf(submitblockpath, sizeof(submitblockpath), "%s/datum_submitblock_%s.json", datum_config.mining_save_submitblocks_dir, block_hash_hex) < sizeof(submitblockpath)) {
			FILE *f = fopen(submitblockpath, "w");
			if (f) { fwrite(submitblock_req, ptr-submitblock_req, 1, f); fclose(f); }
		}
	}
	
	if (!(tcurl = curl_easy_init())) return 0;
	if (submitblock_rpc(tcurl, NULL, submitblock_req, block_hash_hex)) {
		preciousblock_local(tcurl, block_hash_hex);
		ret = 1;
	}
	for (i = 0; i < (size_t)datum_config.extra_block_submissions_count; i++) {
		if (submitblock_rpc(tcurl, datum_config.extra_block_submissions_urls[i], submitblock_req, block_hash_hex)) {
			ret = 1;
		}
	}
	curl_easy_cleanup(tcurl);
	
	if (free_submitblock_req) {
		free(submitblock_req);
	}
	return ret;
}

char* datum_stratum_get_workers_json(void) {
	json_t *root = json_array();
	if (!global_stratum_app) return json_dumps(root, JSON_COMPACT);
	
	int i, ii;
	uint64_t tsms = current_time_millis();
	
	for(i = 0; i < global_stratum_app->max_threads; i++) {
		for(ii = 0; ii < global_stratum_app->max_clients_thread; ii++) {
			if (global_stratum_app->datum_threads[i].client_data[ii].fd > 0) {
				T_DATUM_MINER_DATA *m = global_stratum_app->datum_threads[i].client_data[ii].app_client_data;
				if (m && m->subscribed) {
					json_t *worker = json_object();
					
					// 隐私：不再发送 name (last_auth_username)
					json_object_set_new(worker, "user_agent", json_string(m->useragent[0] ? m->useragent : "Unknown"));
					json_object_set_new(worker, "best_diff", json_real(m->best_share_diff));
					
					double hr = 0.0;
					unsigned char astat = m->stats.active_index ? 0 : 1;
					if ((m->stats.last_swap_ms > 0) && (m->stats.diff_accepted[astat] > 0)) {
						hr = ((double)m->stats.diff_accepted[astat] / (double)((double)m->stats.last_swap_ms / 1000.0)) * 0.004294967296;
					}
					// 超过 180 秒无提交视为掉线 (算力归零)
					if (((double)(tsms - m->stats.last_swap_tsms) / 1000.0) >= 180.0) {
						hr = 0.0; 
					}
					json_object_set_new(worker, "hashrate", json_real(hr));
					
					json_array_append_new(root, worker);
				}
			}
		}
	}
	char *json_str = json_dumps(root, JSON_COMPACT);
	json_decref(root);
	return json_str;
}
