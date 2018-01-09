/*
 * main.c
 *
 *  Created on: Dec 13, 2017
 *      Author: root
 */

#include <stdlib.h>
#include <stdio.h>
#include <groonga/groonga.h>
#include <time.h>
#include <sys/stat.h>
#include <time.h>
#include <sys/sysinfo.h>
#include <sys/time.h>
#include <sys/resource.h>
#include "parson.h"

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

grn_ctx* ctx_insert;
grn_ctx* ctx_remove;
grn_obj* db_insert;
grn_obj* db_remove;
int run;

typedef enum TableType
{
	MAIN,
	REQUEST,
	RESPONSE,
} table_type_t;

typedef struct columns_preset
{
	char* name;
	table_type_t table_type;
	char* data_type;
} columns_preset_t;

static columns_preset_t columns_per_table[] =
{
		{END_TIMESTAMP_COLUMN, MAIN, UINT64_TYPE},
		{SRC_IP_COLUMN, MAIN, UINT32_TYPE},
		{SRC_PORT_COLUMN, MAIN, UINT16_TYPE},
		{SRC_MAC_COLUMN, MAIN, UINT64_TYPE},
		{DST_IP_COLUMN, MAIN, UINT32_TYPE},
		{DST_PORT_COLUMN, MAIN, UINT16_TYPE},
		{DST_MAC_COLUMN, MAIN, UINT64_TYPE},
		{PROTOCOL_ID_COLUMN, MAIN, UINT32_TYPE},
		{RETURN_CODE_COLUMN, MAIN, UINT32_TYPE},
		{REQUEST_CODE_COLUMN, MAIN, UINT32_TYPE},
		{REQUEST_COLUMN, MAIN, SHORT_TEXT_TYPE},
		{RESPONSE_COLUMN, MAIN, SHORT_TEXT_TYPE},
		{TOTAL_BYTE_COLUMN, MAIN, UINT64_TYPE},
		{ART_COLUMN, MAIN, UINT32_TYPE},
		{EURT_COLUMN, MAIN, UINT32_TYPE},
		{NW_DELAY_COLUMN, MAIN, UINT64_TYPE},
		{RETRANS_COLUMN, MAIN, UINT32_TYPE},
		{ZWIN_COLUMN, MAIN, UINT32_TYPE},
		{PID_COLUMN, MAIN, UINT32_TYPE},
		{INDEX_REQ_COLUMN, REQUEST, NULL},
		{INDEX_RES_COLUMN, RESPONSE, NULL},
};

typedef struct groonga_conf
{
	grn_ctx ctx;
	grn_obj *db;
}groonga_conf_t;

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

typedef struct groonga_record
{
	groonga_key_t key;
	groonga_data_t value;
}groonga_record_t;


void check_memory_usage(char* message) {
        struct rusage r;
        getrusage(RUSAGE_SELF, &r);
        printf("[grng]%s:Max memory usage was %lf MB\n", message, (double) r.ru_maxrss / 1000.0);
}


int create_grn_table(grn_ctx* ctx, char *_table_name, char *_flags, char *_key_type, char *_normalizer, char *_tokenizer)
{
	grn_obj *name, *type, *flag, *norm, *token;
	grn_obj *command, *table;

	char* result;
	uint32_t result_length;
	int32_t recv_flags;

	table = grn_ctx_get(ctx, _table_name, strlen(_table_name));
	command = grn_ctx_get(ctx, "table_create", strlen("table_create"));
	if(command == NULL)
		return 0;
	name = grn_expr_get_var(ctx, command, "name", strlen("name"));
	flag = grn_expr_get_var(ctx, command, "flags", strlen("flags"));
	type = grn_expr_get_var(ctx, command, "key_type", strlen("key_type"));
	norm = grn_expr_get_var(ctx, command, "normalizer", strlen("normalizer"));
	token = grn_expr_get_var(ctx, command, "default_tokenizer", strlen("default_tokenizer"));

	grn_obj_reinit(ctx, name, GRN_DB_TEXT, 0);
	grn_obj_reinit(ctx, flag, GRN_DB_TEXT, 0);
	grn_obj_reinit(ctx, type, GRN_DB_TEXT, 0);
	grn_obj_reinit(ctx, norm, GRN_DB_TEXT, 0);
	grn_obj_reinit(ctx, token, GRN_DB_TEXT, 0);

	GRN_TEXT_PUTS(ctx, name, _table_name);
	GRN_TEXT_PUTS(ctx, type, _key_type);
	GRN_TEXT_PUTS(ctx, flag, _flags);
	GRN_TEXT_PUTS(ctx, norm, _normalizer);
	GRN_TEXT_PUTS(ctx, token, _tokenizer);

	grn_expr_exec(ctx, command, 0);
	grn_ctx_recv(ctx, &result, &result_length, &recv_flags);
//	grn_expr_clear_vars(ctx, command);
    grn_obj_unlink(ctx, command);
    grn_obj_close(ctx, table);
	return memcmp(result, "true", result_length) == 0;
}

int create_grn_column(grn_ctx* ctx, char* _table, char* _column, char* _flags, char* _type, char* _sources)
{
	grn_obj *table, *name, *flag, *type, *source;
	grn_obj *command;

	char* result;
	uint32_t length;
	int32_t recv_flags;

	command = grn_ctx_get(ctx, "column_create", strlen("column_create"));
	table = grn_expr_get_var(ctx, command, "table", strlen("table"));
	name = grn_expr_get_var(ctx, command, "name", strlen("name"));
	flag = grn_expr_get_var(ctx, command, "flags", strlen("flags"));
	type = grn_expr_get_var(ctx, command, "type", strlen("type"));
	source = grn_expr_get_var(ctx, command, "source", strlen("source"));

	grn_obj_reinit(ctx, table, GRN_DB_TEXT, 0);
	grn_obj_reinit(ctx, name, GRN_DB_TEXT, 0);
	grn_obj_reinit(ctx, flag, GRN_DB_TEXT, 0);
	grn_obj_reinit(ctx, type, GRN_DB_TEXT, 0);
	grn_obj_reinit(ctx, source, GRN_DB_TEXT, 0);

	GRN_TEXT_PUTS(ctx, table, _table);
	GRN_TEXT_PUTS(ctx, name, _column);
	GRN_TEXT_PUTS(ctx, flag, _flags);
	GRN_TEXT_PUTS(ctx, type, _type);
	GRN_TEXT_PUTS(ctx, source, _sources);

	grn_expr_exec(ctx, command, 0);
	grn_ctx_recv(ctx, &result, &length, &recv_flags);
	grn_expr_clear_vars(ctx, command);
    grn_obj_unlink(ctx, command);
	return memcmp(result, "true", length) == 0;
}

void create_record_table(grn_ctx* ctx, char* name)
{
	grn_obj *index_req, *index_res, *table;
	int res = 0;
	char index_req_name[30], index_res_name[30];
	memset(index_req_name, 0, 30);
	memset(index_res_name, 0, 30);

	sprintf(index_req_name, "%s_%s", name, INDEX_REQUEST);
	sprintf(index_res_name, "%s_%s", name, INDEX_RESPONSE);

	if((table = grn_ctx_get(ctx, name, strlen(name))) == NULL)
	{
		res = create_grn_table(ctx, name, TABLE_HASH_KEY_TYPE, UINT64_TYPE, "", "");
		if(res) printf("create %s table\n", name);
		table = grn_ctx_get(ctx, name, strlen(name));
	}
	if((index_req = grn_ctx_get(ctx, index_req_name, strlen(index_req_name))) == NULL)
	{
		res = create_grn_table(ctx, index_req_name, TABLE_PAT_KEY_TYPE, SHORT_TEXT_TYPE, NORMALIZER_TYPE, TOKEN_BIGRAM);
		if(res) printf("create %s table\n", index_req_name);
		index_req = grn_ctx_get(ctx, index_req_name, strlen(index_req_name));
	}
	if((index_res = grn_ctx_get(ctx, index_res_name, strlen(index_res_name))) == NULL)
	{
		res = create_grn_table(ctx, index_res_name, TABLE_PAT_KEY_TYPE, SHORT_TEXT_TYPE, NORMALIZER_TYPE, TOKEN_BIGRAM);
		if(res) printf("create %s table\n", index_res_name);
		index_res = grn_ctx_get(ctx, index_res_name, strlen(index_res_name));
	}
	int i = 0;

	for(i = 0; i < sizeof(columns_per_table)/sizeof(columns_preset_t); i++)
	{
		switch(columns_per_table[i].table_type)
		{
		case MAIN:
			if(table != NULL)
			{
				res = create_grn_column(ctx, name, columns_per_table[i].name, COLUMN_SCALAR_TYPE, columns_per_table[i].data_type, "");
			}
			break;
		case REQUEST:
			if(index_req != NULL){
				res = create_grn_column(ctx, index_req_name, columns_per_table[i].name, INDEX_FLAG_FOR_SINGLE, name, REQUEST_COLUMN);
			}
			break;
		case RESPONSE:
			if(index_res != NULL){
				res = create_grn_column(ctx, index_res_name, columns_per_table[i].name, INDEX_FLAG_FOR_SINGLE, name, RESPONSE_COLUMN);
			}
			break;
		default:
			break;
		}

		if(res)
		{
			printf("%s is created.\n", columns_per_table[i].name);
		}
		else
		{
			printf("%s\n", ctx->errbuf);
		}
	}


	grn_obj_unlink(ctx, table);
	grn_obj_unlink(ctx, index_req);
	grn_obj_unlink(ctx, index_res);

}

int load_data(grn_ctx* ctx, grn_obj* db, char* value, char* table_name)
{
	uint32_t result_length;
	grn_obj *command, *values, *table, *type;
	int32_t recv_flags;
	char* result;
	command = grn_ctx_get(ctx, "load", strlen("load"));
	while(command == NULL)
	{
		printf("Failed to call groonga command\n");
		return 0;
	}
	table = grn_expr_get_var(ctx, command, "table", strlen("table"));
	values = grn_expr_get_var(ctx, command, "values", strlen("values"));
	type = grn_expr_get_var(ctx, command, "input_type", strlen("input_type"));

	grn_obj_reinit(ctx, table, GRN_DB_TEXT, 0);
	grn_obj_reinit(ctx, values, GRN_DB_TEXT, 0);
	grn_obj_reinit(ctx, type, GRN_DB_TEXT, 0);

	GRN_TEXT_PUTS(ctx, table, table_name);
	GRN_TEXT_PUTS(ctx, values, value);
	GRN_TEXT_PUTS(ctx, type, "json");

//	check_memory_usage("Start grn_expr_exec");
	grn_expr_exec(ctx, command, 0);
//	check_memory_usage("End grn_expr_exec");
	grn_ctx_recv(ctx, &result, &result_length, &recv_flags);
	grn_expr_clear_vars(ctx, command);
	grn_obj_unlink(ctx, command);


	char str_return[result_length+ 1];
	memset(str_return, 0, result_length + 1);
	memcpy(str_return, result, result_length);
	int num = atoi(str_return);
	return num;

}

uint64_t set_record_to_json(groonga_record_t* records, JSON_Value** root, int index)
{
	JSON_Array* array = json_value_get_array(*root);
	JSON_Value *record = json_array_get_value(array, index); //json_value_init_object();
	JSON_Object *array_item = json_value_get_object(record);
	char *p;
	json_object_set_number(array_item, "_key", records->key.start_timestamp);
	json_object_set_number(array_item, END_TIMESTAMP_COLUMN, records->key.end_timestamp);
	json_object_set_number(array_item, SRC_IP_COLUMN, records->key.src_host.ip_addr);
	json_object_set_number(array_item, DST_IP_COLUMN, records->key.dst_host.ip_addr);
	json_object_set_number(array_item, SRC_PORT_COLUMN, records->key.src_host.port);
	json_object_set_number(array_item, DST_PORT_COLUMN, records->key.dst_host.port);
	json_object_set_number(array_item, PROTOCOL_ID_COLUMN, records->value.protocol_id);
	json_object_set_number(array_item, RETURN_CODE_COLUMN, records->value.return_code);
	json_object_set_number(array_item, REQUEST_CODE_COLUMN, records->value.request_code);
	json_object_set_number(array_item, ART_COLUMN, records->value.stats.art);
	json_object_set_number(array_item, EURT_COLUMN, records->value.stats.eurt);
	json_object_set_number(array_item, TOTAL_BYTE_COLUMN, (uint64_t) (records->value.stats.in_byte + records->value.stats.out_byte));
	json_object_set_number(array_item, NW_DELAY_COLUMN, records->value.stats.nw_delay);
	json_object_set_number(array_item, RETRANS_COLUMN, records->value.stats.retrans);
	json_object_set_number(array_item, ZWIN_COLUMN, records->value.stats.zwin);
	json_object_set_number(array_item, SRC_MAC_COLUMN, records->key.src_host.mac_addr);
	json_object_set_number(array_item, DST_MAC_COLUMN, records->key.dst_host.mac_addr);

	if(records->value.request_len > 0){
			while((p = strchr(records->value.request, '"')) != NULL) *p='_';
			char request[records->value.request_len + 1];
			request[records->value.request_len] = '\0';
			memcpy(request, records->value.request, records->value.request_len);
			json_object_set_string(array_item, REQUEST_COLUMN, request);
	}

	if(records->value.response_len > 0){
			while((p = strchr(records->value.response, '"')) != NULL) *p='_';
			char response[records->value.response_len + 1];
			response[records->value.response_len] = '\0';
			memcpy(response, records->value.response, records->value.response_len);
			json_object_set_string(array_item, RESPONSE_COLUMN, response);
	}

	usleep(100);
	return records->key.start_timestamp;
}

int initialize_json_objects(JSON_Value **json_root)
{
        *json_root = json_value_init_array();
        return (*json_root != NULL);
}

int cleanup_json_array(JSON_Array* array)
{
        int count = json_array_get_count(array);
        int i = 0;
        JSON_Status status;
        check_memory_usage("Start to clear array");
        if(count > 0)
        {
                status = json_array_clear(array);
        }
        check_memory_usage("removed all array items.");
        printf("%d\n", (int)json_array_get_count(array));

        for (i = 0; i < 200; i++)
        {
                status = json_array_append_value(array, json_value_init_object());
        }

        check_memory_usage("Restore empty array");
        return status == JSONSuccess;
}

int main(int argc, char** argv)
{
	grn_init();
	grn_ctx* insert = grn_ctx_open(0);
	grn_obj* db;
	grn_ctx_init(insert, 0);
	GRN_DB_OPEN_OR_CREATE(insert, "/opt/uila/db/uila.db", 0, db);

	int j = 0;
	char name[11];
    char freetext[80];
    groonga_record_t record;
    JSON_Value* json_root;
    initialize_json_objects(&json_root);
	cleanup_json_array(json_value_get_array(json_root));

	unsigned long long timestamp = atoll(argv[1]);

	memset(name, 0, 11);
	sprintf(name, "%lld", timestamp);
	printf("create %s\n", name);
	create_record_table(insert, name);
	srand((unsigned int)time(NULL));

	int len = 0;

	while(1){
		check_memory_usage("start to create records");
		len = 0;
		while(len < 200)
		{
			int rd = rand();
			for(j = 0; j < 79; j++)
			{
				rd = rand();
				freetext[j] = (uint8_t)(rd % (80-33) + 33);
			}
	    	record.key.start_timestamp = (uint64_t)timestamp + rd + len;
	    	record.key.end_timestamp = (uint64_t)timestamp + rd + len + 20;
	    	record.key.dst_host.ip_addr = (12345 << 2) + rd % 1000;
	    	record.key.src_host.ip_addr = (12345 << 1) + rd % 1000;
	    	record.key.dst_host.mac_addr = (uint64_t)(54321 << 2) + rd % 1000;
	    	record.key.src_host.mac_addr = (uint64_t)(54321 << 1) + rd % 1000;
	    	record.key.dst_host.port = 80;
	    	record.key.src_host.port = 4000 + rd % 1000;
	    	record.value.protocol_id = 67;
	    	record.value.request_len = 128;
	    	record.value.response_len = 128;
	    	record.value.return_code = 200 + rd % 200;
	    	record.value.request_code = rd % 10;
	    	record.value.stats.art = 0;
	    	record.value.stats.eurt = rd % 100;
	    	record.value.stats.in_byte = rd % 1000;
	    	record.value.stats.out_byte = rd % 1000;
	    	record.value.stats.nw_delay = (uint64_t)rd % 50;
	    	record.value.stats.retrans = rd % 300;
	    	record.value.stats.zwin = rd % 150;
	    	sprintf(record.value.request, "HTTP %s", freetext);
	    	sprintf(record.value.response, "200 OK %s", freetext);

//			check_memory_usage("set record to array.");
			set_record_to_json(&record, &json_root, len);
			memset(freetext, 0, 80);
			memset(record.value.request, 0, 128);
			memset(record.value.response, 0, 128);
			len++;
		}

		check_memory_usage("start to insert");
		char* record_text = json_serialize_to_string_pretty(json_root);
		int num = load_data(insert, db, record_text, name);
		printf("insert %d records to %s\n", num, name);
		cleanup_json_array(json_value_get_array(json_root));
		free(record_text);

		check_memory_usage("End of loop");
		sleep(1);
	}
	grn_obj_close(insert, db);
	grn_ctx_fin(insert);
	grn_fin();
	return 0;
}
