#include <errno.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include <zmq.h>

#include "datum_blocktemplates.h"
#include "datum_conf.h"
#include "datum_utils.h"
#include "datum_zmq.h"

#define DATUM_ZMQ_HASHBLOCK_TOPIC "hashblock"

void *datum_zmq_hashblock_thread(void *arg) {
	bool have_sequence = false;
	uint32_t last_sequence = 0;

	(void)arg;

	while (1) {
		void *ctx = NULL;
		void *sock = NULL;
		int linger = 0;
#ifdef ZMQ_IPV6
		int ipv6 = 1;
#endif

		if (!datum_config.bitcoind_zmq_hashblock_url[0]) {
			sleep(1);
			continue;
		}

		ctx = zmq_ctx_new();
		if (!ctx) {
			DLOG_WARN("Could not create ZMQ context for hashblock subscriber: %s", zmq_strerror(errno));
			sleep(5);
			continue;
		}

		sock = zmq_socket(ctx, ZMQ_SUB);
		if (!sock) {
			DLOG_WARN("Could not create ZMQ SUB socket for hashblock subscriber: %s", zmq_strerror(errno));
			zmq_ctx_term(ctx);
			sleep(5);
			continue;
		}

		zmq_setsockopt(sock, ZMQ_LINGER, &linger, sizeof(linger));
#ifdef ZMQ_IPV6
		zmq_setsockopt(sock, ZMQ_IPV6, &ipv6, sizeof(ipv6));
#endif
		if (zmq_setsockopt(sock, ZMQ_SUBSCRIBE, DATUM_ZMQ_HASHBLOCK_TOPIC, strlen(DATUM_ZMQ_HASHBLOCK_TOPIC)) != 0) {
			DLOG_WARN("Could not subscribe to ZMQ hashblock topic: %s", zmq_strerror(errno));
			zmq_close(sock);
			zmq_ctx_term(ctx);
			sleep(5);
			continue;
		}
		if (zmq_connect(sock, datum_config.bitcoind_zmq_hashblock_url) != 0) {
			DLOG_WARN("Could not connect ZMQ hashblock subscriber to %s: %s", datum_config.bitcoind_zmq_hashblock_url, zmq_strerror(errno));
			zmq_close(sock);
			zmq_ctx_term(ctx);
			sleep(5);
			continue;
		}

		DLOG_INFO("Connected ZMQ hashblock subscriber to %s", datum_config.bitcoind_zmq_hashblock_url);

		while (1) {
			uint8_t body[32];
			uint8_t seqbuf[4];
			size_t topic_len = 0;
			size_t body_len = 0;
			size_t seq_len = 0;
			int more = 0;
			size_t more_size = sizeof(more);
			char topic[32];
			char block_hash_hex[65];
			int rc;

			memset(topic, 0, sizeof(topic));
			memset(body, 0, sizeof(body));
			memset(seqbuf, 0, sizeof(seqbuf));
			memset(block_hash_hex, 0, sizeof(block_hash_hex));

			rc = zmq_recv(sock, topic, sizeof(topic) - 1, 0);
			if (rc < 0) {
				if (errno == EINTR) continue;
				DLOG_WARN("ZMQ hashblock subscriber receive failed on topic frame: %s", zmq_strerror(errno));
				break;
			}
			topic_len = (size_t)rc;
			if (zmq_getsockopt(sock, ZMQ_RCVMORE, &more, &more_size) != 0 || !more) {
				DLOG_WARN("ZMQ hashblock subscriber received malformed topic-only message");
				break;
			}

			rc = zmq_recv(sock, body, sizeof(body), 0);
			if (rc < 0) {
				if (errno == EINTR) continue;
				DLOG_WARN("ZMQ hashblock subscriber receive failed on body frame: %s", zmq_strerror(errno));
				break;
			}
			body_len = (size_t)rc;
			if (zmq_getsockopt(sock, ZMQ_RCVMORE, &more, &more_size) != 0 || !more) {
				DLOG_WARN("ZMQ hashblock subscriber received message without sequence frame");
				break;
			}

			rc = zmq_recv(sock, seqbuf, sizeof(seqbuf), 0);
			if (rc < 0) {
				if (errno == EINTR) continue;
				DLOG_WARN("ZMQ hashblock subscriber receive failed on sequence frame: %s", zmq_strerror(errno));
				break;
			}
			seq_len = (size_t)rc;

			if (topic_len != strlen(DATUM_ZMQ_HASHBLOCK_TOPIC) || strcmp(topic, DATUM_ZMQ_HASHBLOCK_TOPIC) != 0) {
				continue;
			}
			if (body_len != sizeof(body)) {
				DLOG_WARN("Ignoring malformed ZMQ hashblock body (%u bytes)", (unsigned int)body_len);
				continue;
			}
			if (seq_len == sizeof(seqbuf)) {
				const uint32_t seq = upk_u32le(seqbuf, 0);
				if (have_sequence && seq != (last_sequence + 1)) {
					DLOG_WARN("ZMQ hashblock sequence gap detected: expected %u, got %u", last_sequence + 1, seq);
				}
				last_sequence = seq;
				have_sequence = true;
			}

			hash2hex(body, block_hash_hex);
			DLOG_INFO("ZMQ hashblock notification received: %s", block_hash_hex);
			datum_blocktemplates_notifynew(block_hash_hex, 0);
		}

		zmq_close(sock);
		zmq_ctx_term(ctx);
		DLOG_WARN("ZMQ hashblock subscriber disconnected, retrying");
		sleep(5);
	}

	return NULL;
}
