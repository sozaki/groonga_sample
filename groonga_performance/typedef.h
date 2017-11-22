/*
 * typedef.h
 *
 *  Created on: Aug 23, 2017
 *      Author: ozakis
 */

#ifndef TYPEDEF_H_
#define TYPEDEF_H_

#include <groonga.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <malloc.h>
#include <signal.h>
#include <unistd.h>
#include <dirent.h>
#include <errno.h>
#include <sys/statvfs.h>
#include <sys/types.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include "parson.h"

#define DB_PATH "/tmp/uila/record.db"
#define INDEX_REQUEST "index_request"
#define INDEX_RESPONSE "index_response"
#define END_TIMESTAMP_COLUMN "end_timestamp"
#define SRC_IP_COLUMN "src_ip"
#define DST_IP_COLUMN "dst_ip"
#define SRC_PORT_COLUMN "src_port"
#define DST_PORT_COLUMN "dst_port"
#define SRC_MAC_COLUMN "src_mac"
#define DST_MAC_COLUMN "dst_mac"
#define REQUEST_COLUMN "request"
#define RESPONSE_COLUMN "response"
#define PROTOCOL_ID_COLUMN "protocol_id"
#define RETURN_CODE_COLUMN "return_code"
#define REQUEST_CODE_COLUMN "request_code"
#define STATE_COLUMN "state"
#define TOTAL_BYTE_COLUMN "total_bytes"
#define ART_COLUMN "ART"
#define EURT_COLUMN "EURT"
#define NW_DELAY_COLUMN "ND"
#define RETRANS_COLUMN "Retrans"
#define ZWIN_COLUMN "Zwin"
#define PID_COLUMN "PID"
#define INDEX_REQ_COLUMN "index_req_col"
#define INDEX_RES_COLUMN "index_res_col"

#define TABLE_HASH_KEY_TYPE "TABLE_HASH_KEY|KEY_LARGE"
#define TABLE_PAT_KEY_TYPE "TABLE_PAT_KEY"
#define COLUMN_SCALAR_TYPE "COLUMN_SCALAR"
#define SHORT_TEXT_TYPE "ShortText"
#define UINT64_TYPE "UInt64"
#define UINT32_TYPE "UInt32"
#define INT32_TYPE "Int32"
#define UINT16_TYPE "UInt16"
#define NORMALIZER_TYPE "NormalizerAuto"
#define TOKEN_BIGRAM "TokenBigram"
#define INDEX_FLAG_FOR_SINGLE "COLUMN_INDEX|WITH_POSITION"
#define INVALID_COLUMNS_LEN 10

#define NUM_OF_RECORDS 99
#define ARRAY_SIZE NUM_OF_RECORDS
#define POOL_SIZE 200
#define TABLE_SIZE 24
#define WAIT_TIME 60000000
#define DISK_LIMIT 80
#define TIMEOUT 1

#define ENABLE_INSERT 35
#define RLIMIT_SIZE_UNIT 1024

enum NAME_MODE
{
	PER_DAY,
	PER_HOUR,
	PER_MIN,
	PER_SEC,
};

typedef struct host
{
	uint32_t ip_addr;
	uint64_t mac_addr;
	uint16_t port;
} host_t;

typedef struct groonga_key
{
	uint64_t start_timestamp;
	uint64_t end_timestamp;
	host_t src_host;
	host_t dst_host;
} groonga_key_t;

typedef struct groonga_stats
{
	uint32_t in_byte;
	uint32_t out_byte;
	uint32_t art;
	uint32_t eurt;
	uint64_t nw_delay;
	uint32_t retrans;
	uint32_t zwin;
} groonga_stats_t;

typedef struct groonga_raw
{
	char request[128];
	char response[128];
	uint16_t request_len;
	uint16_t response_len;
	uint32_t protocol_id;
	uint32_t return_code;
	uint32_t request_code;
	char state[128];
	groonga_stats_t stats;
} groonga_data_t;

typedef struct groonga_conf
{
	grn_ctx ctx;
	grn_obj *db;
}groonga_conf_t;

typedef struct groonga_record
{
	groonga_key_t key;
	groonga_data_t value;
}groonga_record_t;

typedef struct groonga_table
{
	char name[11];
	uint64_t ll_table_name;
}groonga_table_t;

typedef struct groonga_table_buffer
{
	groonga_table_t* table;
	int current_index;
}groonga_table_buffer_t;

typedef struct groonga_data_buffer
{
	JSON_Array *array;
	JSON_Value *json_root;
	char* serialized_string;
	bool is_used;
	bool is_inserting;
	int pool_index;
	int array_size;
	uint64_t last_timestamp;
} groonga_data_buffer_t;

#endif /* TYPEDEF_H_ */
