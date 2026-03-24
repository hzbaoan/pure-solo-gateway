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

#include <assert.h>
#include <sys/types.h>
#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <math.h>
#include <stdbool.h>
#include <ctype.h>
#include <string.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/time.h>
#include <inttypes.h>
#include <unistd.h>
#include <limits.h>
#include <sodium.h>
#include "datum_logger.h"
#include "datum_utils.h"
#include "thirdparty_base58.h"
#include "thirdparty_segwit_addr.h"

volatile int panic_mode = 0;
static uint64_t process_start_time = 0;

void get_target_from_diff(unsigned char *result, uint64_t diff) {
	uint64_t dividend_parts[4] = {0, 0, 0, 0x00000000FFFF0000};
	uint64_t remainder = 0;
	uint64_t quotient;
	
	memset(result, 0, 32);
	
	for (int i = 3; i >= 0; i--) {
		__uint128_t temp = remainder;
		temp = (temp << 64) | dividend_parts[i];
		
		quotient = temp / diff;
		remainder = temp % diff;
		
		for (int j = 0; j < 8; j++) {
			result[(i<<3) + j] = (quotient >> (j<<3)) & 0xFF;
		}
	}
}
uint64_t get_process_uptime_seconds() {
	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC, &ts);
	return (uint64_t)ts.tv_sec - process_start_time;
}

void datum_utils_init(void) {
	build_hex_lookup();
	process_start_time = monotonic_time_seconds();
}

#ifdef __GNUC__
// faster, less portable
uint64_t roundDownToPowerOfTwo_64(uint64_t x) {
	return 1ULL << (63 - __builtin_clzll(x));
}

unsigned char floorPoT(uint64_t x) {
	if (x == 0) {
		return 0;
	}
	
	return (63 - __builtin_clzll(x));
}

#else
// More portable but slower
uint64_t roundDownToPowerOfTwo_64(uint64_t x) {
	x |= x >> 1;
	x |= x >> 2;
	x |= x >> 4;
	x |= x >> 8;
	x |= x >> 16;
	x |= x >> 32;
	return x - (x >> 1);
}

unsigned char floorPoT(uint64_t x) {
	if (x == 0) {
		return 0;
	}
	
	unsigned char pos = 0;
	while (x >>= 1) {
		pos++;
	}
	return pos;
}
#endif

uint64_t monotonic_time_seconds(void) {
	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC, &ts); // SAFE from system time changes (e.g., NTP adjustments, manual clock changes)
	return (uint64_t)ts.tv_sec;
}

uint64_t current_time_millis(void) {
	struct timeval te;
	gettimeofday(&te, NULL); // get current time
	uint64_t milliseconds = te.tv_sec * 1000LL + te.tv_usec / 1000; // calculate milliseconds
	return milliseconds;
}

uint64_t current_time_micros(void) {
	struct timeval te;
	gettimeofday(&te, NULL); // get current time
	uint64_t microseconds = te.tv_sec * 1000000LL + te.tv_usec; // calculate microseconds
	return microseconds;
}

unsigned char hex_lookup[65536];
unsigned short uchar_hex_lookup[256];

unsigned char hex2bin_uchar(const char *in) {
	unsigned short i;
	memcpy(&i, in, sizeof(i));
	return hex_lookup[i];
}

void uchar_to_hex(char *s, const unsigned char b) {
	// place the ASCII hexidecimal value for the unsigned char into the string at ptr s
	// this does NOT null terminate the string!
	unsigned short i = uchar_hex_lookup[b];
	memcpy(s, &i, sizeof(i));
}

void build_hex_lookup(void) {
	unsigned int i;
	char a[3];
	unsigned short b;
	for(i = 0; i < 65535; ++i) hex_lookup[i] = 0;
	hex_lookup[65535] = 0;
	for(i=0;i<256;i++) {
		sprintf(a,"%2.2X",i);
		memcpy(&b, a, sizeof(b));
		hex_lookup[b] = i;
		sprintf(a,"%2.2x",i);
		memcpy(&b, a, sizeof(b));
		hex_lookup[b] = i;
		uchar_hex_lookup[i] = b;
	}
}

bool my_sha256(void *digest, const void *buffer, size_t length) {
	crypto_hash_sha256(digest, buffer, length);
	return 1;
}

bool double_sha256(void *out, const void *in, size_t length) {
	unsigned char dg1[32];
	my_sha256(dg1, in, length);
	my_sha256(out, dg1, 32);
	return 1;
}

void nbits_to_target(uint32_t nbits, uint8_t *target) {
	uint32_t exponent = (nbits >> 24) & 0xff;
	uint32_t mantissa = nbits & 0xffffff;
	int i;
	
	memset(target, 0, 32);
	if (exponent <= 3) {
		mantissa >>= 8 * (3 - exponent);
		for (i = 0; i < 3; i++) {
			target[i] = (mantissa >> (8 * i)) & 0xff;
		}
	} else {
		for (i = 0; i < 3; i++) {
			target[i + exponent - 3] = (mantissa >> (8 * i)) & 0xff;
		}
	}
}

int compare_hashes(const uint8_t *share_hash, const uint8_t *target) {
	for (int i = 31; i >= 0; i--) {
		if (share_hash[i] < target[i]) return -1; // share_hash is smaller, valid block/share
		if (share_hash[i] > target[i]) return 1;  // share_hash is larger, not valid
	}
	return 0; // hashes are equal
}

int append_bitcoin_varint_hex(uint64_t n, char *s) {
	if (n < 0xFD) {
		// Single byte is sufficient
		uchar_to_hex(s, n);
		s[2] = 0;
		return 2;
	} else if (n <= 0xFFFF) {
		// Use 0xFD followed by the number, little-endian order
		sprintf(s, "fd%04" PRIx16, __builtin_bswap16((uint16_t)n));
		return 6;
	} else if (n <= 0xFFFFFFFF) {
		// Use 0xFE followed by the number, little-endian order
		sprintf(s, "fe%08" PRIx32, __builtin_bswap32((uint32_t)n));
		return 10;
	} else {
		// Use 0xFF followed by the number, little-endian order
		sprintf(s, "ff%016" PRIx64, __builtin_bswap64(n));
		return 18;
	}
}

int append_UNum_hex(uint64_t n, char *s) {
	int count = 0;
	uint64_t temp = n;
	bool last_msb = false;
	
	do {
		count++;
		temp >>= 8;
	} while (temp != 0);
	
	int len = 2;
	uchar_to_hex(s, count);
	
	for (int i = 0; i < count; i++) {
		uchar_to_hex(s+len, (uint8_t)(n & 0xFF));
		
		last_msb = (n >= 0x80);
		
		n >>= 8;
		len += 2;
	}
	
	// if the last byte is >= 0x80, then we need to inject a zero at the end
	if (last_msb) {
		count++;
		uchar_to_hex(s, count);
		uchar_to_hex(s+len, 0);
		len+=2;
	}
	
	s[len] = '\0';
	
	return len;
}

void hex_to_bin_le(const char *hex, unsigned char *bin) {
	size_t len = strlen(hex);
	for (size_t i = 0; i < len>>1; i++) {
		bin[i] = hex2bin_uchar(&hex[len - ((i+1)<<1)]);
	}
}

void panic_from_thread(int a) {
	// set panic flag
	panic_mode = 1;
	
	printf("PANIC TRIGGERED - %d\n",a);
	fflush(stdout);
	
	DLOG_FATAL("***********************");
	DLOG_FATAL("*** PANIC TRIGGERED ***");
	DLOG_FATAL("***********************");
	
	// the main thread needs to pickup on this failure and exit as gracefully as possible.
	while(1) sleep(1);
}

void hash2hex(unsigned char *bytes, char *hexString) {
	const char hexDigits[] = "0123456789abcdef";
	
	for (int i = 0; i < 32; ++i) {
		hexString[i * 2]     = hexDigits[(bytes[i] >> 4) & 0x0F];
		hexString[i * 2 + 1] = hexDigits[bytes[i] & 0x0F];
	}
	
	hexString[64] = '\0';
}

int addr_2_output_script(const char *addr, unsigned char *script, int max_len) {
	// takes any valid bitcoin address, and converts it to an output script
	// returns length of script written, or 0 on failure
	// NOTE: This is agnostic to testnet vs mainnet addresses! be careful with your networks!
	
	int i;
	size_t al;
	uint8_t witprog[80];
	size_t witprog_len;
	int witver;
	const char* hrp = "bc";
	
	al = strlen(addr);
	
	if (al < 16) return 0;
	
	if (((addr[0] == 'b') && (addr[1] == 'c')) || ((addr[0] == 't') && (addr[1] == 'b'))) {
		// bitcoin mainnet and testnet BIP 0173
		if (addr[0] == 't') {
			hrp = "tb";
		}
		i = segwit_addr_decode(&witver, witprog, &witprog_len, hrp, addr);
		if (!i) {
			return 0;
		}
		
		if (!(((witver == 0) && ((witprog_len == 20) || (witprog_len == 32))) || ((witver == 1) && (witprog_len == 32)))) {
			// enforcing length restrictions and known witness versions
			// TODO: Add any new witness version/len combos that are valid
			return 0;
		}
		
		if (max_len < witprog_len+2) {
			return 0;
		}
		
		script[0] = (uint8_t)(witver ? (witver + 0x50) : 0);
		script[1] = (uint8_t)witprog_len;
		memcpy(script + 2, witprog, witprog_len);
		return witprog_len + 2;
	} else {
		// try P2PKH or P2SH
		const size_t sz = blkmk_address_to_script(script, max_len, addr);
		if (sz > INT_MAX) return 0;
		return sz;
	}
	
	// nothing worked?
	return 0;
}

bool strncpy_workerchars(char *out, const char *in, size_t maxlen) {
	// copy a string from in to out, stripping out unwanted characters
	// copy a max of maxlen chars from in to out
	// in could technically be longer than maxlen if it has a bunch of unwanted chars
	
	int i=0;
	int j=0;
	char c;
	
	if (in == NULL || out == NULL || maxlen == 0) {
		return false;
	}
	
	out[0] = 0;
	while((in[i] != 0) && (maxlen > 1)) {
		c = in[i];
		if ((isalnum(c)) || (c == '.') || (c == '-') || (c == '_') || (c == '=') || (c == '@') || (c == ',')) {
			out[j] = c;
			j++;
			maxlen--;
		}
		i++;
	}
	out[j] = 0;
	return true;
}

bool strncpy_uachars(char *out, const char *in, size_t maxlen) {
	// copy a string from in to out, stripping out unwanted characters
	// copy a max of maxlen chars from in to out
	// in could technically be longer than maxlen if it has a bunch of unwanted chars
	
	int i=0;
	int j=0;
	char c;
	
	if (in == NULL || out == NULL || maxlen == 0) {
		return false;
	}
	
	out[0] = 0;
	while((in[i] != 0) && (maxlen > 1)) {
		c = in[i];
		if ((isalnum(c)) || (c == '.') || (c == '-') || (c == '_') || (c == '=') || (c == '@') || (c == ',') || (c == ' ') || (c == '|') || (c == '/') || (c == '|') || (c == ':') || (c == '<') || (c == '>') || (c == '\'') || (c == ';')) {
			out[j] = c;
			j++;
			maxlen--;
		}
		i++;
	}
	out[j] = 0;
	return true;
}

long double calc_network_difficulty(const char *bits_hex) {
	// given a share solution in hex, calculate the network difficulty
	// Postgres code for this (with hex_to_int added function)
	// (pow(10,  ( (29-tpower_val)*2.4082399653118495617099111577959 ) + log( (65535 / tvalue_val) )   )  ) as network_difficulty
	
	char tpower[3];
	char tvalue[7];
	unsigned char tpower_val;
	unsigned long tvalue_val;
	char *ep;
	int i;
	long double d;
	signed short s;
	
	tpower[0] = bits_hex[0];
	tpower[1] = bits_hex[1];
	tpower[2] = 0;
	
	for(i=0;i<6;i++) tvalue[i] = bits_hex[2+i];
	tvalue[6] = 0;
	tpower_val = (unsigned char)strtoul(tpower, &ep, 16);
	tvalue_val = strtoul(tvalue, &ep, 16);
	s = (signed short)29 - (signed short)tpower_val;
	d = powl(10.0,(double)s*(long double)2.4082399653118495617099111577959 + log10(65535.0 / (double)tvalue_val));
	return d;
}

// Uses a fixed-size buffer; positive only; digits only
// Returns UINT64_MAX on failure
uint64_t datum_atoi_strict_u64(const char * const s, const size_t size) {
	if (!size) return UINT64_MAX;
	assert(s);
	uint64_t ret = 0;
	for (size_t i = 0; i < size; ++i) {
		if (s[i] < '0' || s[i] > '9') return UINT64_MAX;
		int digit = s[i] - '0';
		if (ret > (UINT64_MAX - digit) / 10) return UINT64_MAX;
		ret = (ret * 10) + digit;
	}
	return ret;
}

// Uses a fixed-size buffer; positive only; digits only
// Returns -1 on failure
int datum_atoi_strict(const char * const s, const size_t size) {
	const uint64_t ret = datum_atoi_strict_u64(s, size);
	return (ret == UINT64_MAX || ret > INT_MAX) ? -1 : ret;
}

char **datum_deepcopy_charpp(const char * const * const p) {
	size_t sz = sizeof(char*), n = 0;
	for (const char * const *p2 = p; *p2; ++p2) {
		sz += sizeof(char*) + strlen(*p2) + 1;
		++n;
	}
	char **out = malloc(sz);
	char *p3 = (void*)(&out[n + 1]);
	out[n] = NULL;
	for (size_t i = 0; i < n; ++i) {
		const size_t item_sz = strlen(p[i]) + 1;
		memcpy(p3, p[i], item_sz);
		out[i] = p3;
		p3 += item_sz;
	}
	assert(p3 - (char*)out == sz);
	return out;
}
