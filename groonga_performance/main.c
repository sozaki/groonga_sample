/*
 * main.c
 *
 *  Created on: Aug 23, 2017
 *      Author: ozakis
 */

#include <groonga.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdbool.h>
#include "parson.h"
#include "typedef.h"
#include <time.h>

int pid;
groonga_data_buffer_t buf;
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

int get_open_file_count()
{
	char command[100];
	char buf[10];
	sprintf(command, "ls -l /proc/%d/fd | wc -l", pid);
	FILE *fp;
	if((fp = popen(command, "r")) == NULL)
	{
		printf("Failed to get open file count.\n");
	}

	int fd = 0;

	while(fgets(buf, 10, fp) != NULL)
	{
		fd = atoi(buf);
	}

	pclose(fp);
	return fd;
}

void groonga_close_table2(grn_ctx* ctx, char* name)
{
	grn_obj* obj;
	char* column_name[50];
	memset(column_name, 0, 50);
	sprintf(column_name, "%s.ART", name);
	obj = grn_ctx_get(ctx, column_name, strlen(column_name));
	grn_obj_close(ctx, obj);
}

void groonga_close_table(grn_ctx* ctx, grn_obj* table)
{
	int fd = 0;
	fd = get_open_file_count();
    printf("current - %d\n", fd);

    grn_hash *cols;
	if ((cols = grn_hash_create(ctx, NULL, sizeof(grn_id), 0, GRN_OBJ_TABLE_HASH_KEY|GRN_HASH_TINY))) {
		grn_table_columns(ctx, table, NULL, 0, (grn_obj *)cols);
		GRN_HASH_EACH_BEGIN(ctx, cols, cursor, id){
			void* key;
			grn_obj* col;
			grn_id column_id;
			grn_hash_cursor_get_key(ctx, cursor, &key);
			column_id = *((grn_id*)key);
			if (col){
					grn_obj_close(ctx, col);
			}
		} GRN_HASH_EACH_END(ctx, cursor);
	}
	grn_hash_close(ctx, cols);
	grn_obj_close(ctx, table);

	fd = get_open_file_count();
	printf("closed tables: current - %d\n", fd);
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

	grn_expr_exec(ctx, command, 0);
	grn_ctx_recv(ctx, &result, &result_length, &recv_flags);
	grn_expr_clear_vars(ctx, command);
	grn_obj_unlink(ctx, command);

	char str_return[result_length+ 1];
	memset(str_return, 0, result_length + 1);
	memcpy(str_return, result, result_length);
	int num = atoi(str_return);
	return num;
}

int insert_records(grn_ctx* ctx, grn_obj* db, char* array, char* table_name)
{
	if(array == NULL)
	{
		return true;
	}

	int result =  load_data(ctx, db, array, table_name);
	usleep(100);
	return result;
}

uint64_t set_record_to_json(groonga_record_t* records, JSON_Array** array)
{
	JSON_Value *record = json_value_init_object();
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
	json_object_set_number(array_item, ART_COLUMN, records->value.stats.art);
	json_object_set_number(array_item, EURT_COLUMN, records->value.stats.eurt);
	json_object_set_number(array_item, TOTAL_BYTE_COLUMN, (uint64_t)(records->value.stats.in_byte + records->value.stats.out_byte));
	json_object_set_number(array_item, NW_DELAY_COLUMN, records->value.stats.nw_delay);
	json_object_set_number(array_item, RETRANS_COLUMN, records->value.stats.retrans);
	json_object_set_number(array_item, ZWIN_COLUMN, records->value.stats.zwin);
	json_object_set_number(array_item, PID_COLUMN, pid);
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

	json_array_append_value(*array, record);
	return records->key.start_timestamp;
}

bool create_grn_table(grn_ctx* ctx, char *_table_name, char *_flags, char *_key_type, char *_normalizer, char *_tokenizer)
{
	grn_obj *name, *type, *flag, *norm, *token;
	grn_obj *command, *table;

	char* result;
	uint32_t result_length;
	int32_t recv_flags;

	table = grn_ctx_get(ctx, _table_name, strlen(_table_name));
	command = grn_ctx_get(ctx, "table_create", strlen("table_create"));
	if(command == NULL)
		return false;
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
	grn_expr_clear_vars(ctx, command);
    grn_obj_unlink(ctx, command);
    grn_obj_close(ctx, table);
	return memcmp(result, "true", result_length) == 0;
}

bool create_grn_column(grn_ctx* ctx, char* _table, char* _column, char* _flags, char* _type, char* _sources)
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
	char index_req_name[30], index_res_name[30];
	memset(index_req_name, 0, 30);
	memset(index_res_name, 0, 30);

	sprintf(index_req_name, "%s_%s", name, INDEX_REQUEST);
	sprintf(index_res_name, "%s_%s", name, INDEX_RESPONSE);

	if((table = grn_ctx_get(ctx, name, strlen(name))) == NULL)
	{
		create_grn_table(ctx, name, TABLE_HASH_KEY_TYPE, UINT64_TYPE, "", "");
		table = grn_ctx_get(ctx, name, strlen(name));
	}
	if((index_req = grn_ctx_get(ctx, index_req_name, strlen(index_req_name))) == NULL)
	{
		create_grn_table(ctx, index_req_name, TABLE_PAT_KEY_TYPE, SHORT_TEXT_TYPE, NORMALIZER_TYPE, TOKEN_BIGRAM);
		index_req = grn_ctx_get(ctx, index_req_name, strlen(index_req_name));
	}
	if((index_res = grn_ctx_get(ctx, index_res_name, strlen(index_res_name))) == NULL)
	{
		create_grn_table(ctx, index_res_name, TABLE_PAT_KEY_TYPE, SHORT_TEXT_TYPE, NORMALIZER_TYPE, TOKEN_BIGRAM);
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
				create_grn_column(ctx, name, columns_per_table[i].name, COLUMN_SCALAR_TYPE, columns_per_table[i].data_type, "");
			}
			break;
		case REQUEST:
			if(index_req != NULL){
				create_grn_column(ctx, index_req_name, columns_per_table[i].name, INDEX_FLAG_FOR_SINGLE, name, REQUEST_COLUMN);
			}
			break;
		case RESPONSE:
			if(index_res != NULL){
				create_grn_column(ctx, index_res_name, columns_per_table[i].name, INDEX_FLAG_FOR_SINGLE, name, RESPONSE_COLUMN);
			}
			break;
		default:
			break;
		}
	}


	grn_obj_unlink(ctx, table);
	grn_obj_unlink(ctx, index_req);
	grn_obj_unlink(ctx, index_res);

}

bool remove_grn_table(grn_ctx* ctx, char* table_name)
{
	grn_obj *table;
	grn_obj *command;

	char* result;
	uint32_t length;
	int32_t recv_flags;

	int fd = get_open_file_count();
	printf("removing tables: current - %d\n", fd);

	command = grn_ctx_get(ctx, "table_remove", strlen("table_remove"));
	if(command == NULL)
	{
		return false;
	}
	table = grn_expr_get_var(ctx, command, "name", strlen("name"));

	grn_obj_reinit(ctx, table, GRN_DB_TEXT, 0);

	GRN_TEXT_PUTS(ctx, table, table_name);

	grn_expr_exec(ctx, command, 0);
	grn_ctx_recv(ctx, &result, &length, &recv_flags);
	if(memcmp(result, "false", length) == 0)
	{
		printf("%s", ctx->errbuf);
	}

	grn_expr_clear_vars(ctx, command);
    grn_obj_unlink(ctx, command);

	fd = get_open_file_count();
	printf("removed tables: current - %d\n", fd);

	return memcmp(result, "true", length) == 0;
}

int main(int argc, char** argv)
{
	if(argc < 3)
	{
		return -1;
	}

	int hours = atoi(argv[1]);
	int record_per_table = atoi(argv[2]);
	printf("%d: %d\n", hours, record_per_table);
	srand((unsigned int)time(NULL));
	pid = getpid();
	grn_ctx ctx;
	grn_obj* db;
	grn_init();
	grn_ctx_init(&ctx, 0);
	db = grn_db_open(&ctx, "/tmp/uila/db/uila.db");
	grn_rc result = grn_ctx_set_output_type(&ctx, GRN_CONTENT_JSON);
	if(result != GRN_SUCCESS)
	{
		return -1;
	}

	result = grn_set_lock_timeout(100);
	result = grn_set_lock_timeout(1000);

	buf.json_root = json_value_init_array();
	buf.array = json_value_get_array(buf.json_root);
	time_t timer;
	time(&timer);
    struct tm *current_time = localtime(&timer);
    printf("%d: %d: %d, %ld\n", current_time->tm_hour, current_time->tm_min, current_time->tm_sec, (uint64_t)timer);
    uint64_t timestamp;
    int table_num = 0;
    groonga_record_t record;
    memset(record.value.request, 0, 128);
    memset(record.value.response, 0, 128);
    uint64_t i = 0;
    int j = 0;
    char name[11];
    char freetext[80];
    grn_obj *table, *index_res, *index_req;
    while(table_num <= hours)
    {
    	if(i % (record_per_table) == 0)
    	{
    		if(name != NULL && table_num > 0)
    		{
    			table = grn_ctx_get(&ctx, name, strlen(name));
    			groonga_close_table(&ctx, table);
    			//groonga_close_table2(&ctx, name);
    			index_req = grn_ctx_get(&ctx, INDEX_REQUEST, strlen(INDEX_REQUEST));
    			index_res = grn_ctx_get(&ctx, INDEX_RESPONSE, strlen(INDEX_RESPONSE));
    			groonga_close_table(&ctx, index_req);
    			groonga_close_table(&ctx, index_res);
			}

    		printf("%d, %d, %d, %d\n", (int)timer, current_time->tm_yday, current_time->tm_year, (int)timer - (current_time->tm_yday - 1) * 24 * 3600 - current_time->tm_hour * 60 * 60 - current_time->tm_min * 60 - current_time->tm_sec);
    		timestamp =(uint64_t)((int)timer) * 1000000;
			printf("%lu\n", timestamp);
    		sprintf(name, "%10ld",(uint64_t)((int)timer - (current_time->tm_yday - 1) * 24 * 3600 - current_time->tm_hour * 60 * 60 - current_time->tm_min * 60 - current_time->tm_sec) + table_num * 3600);
    		create_record_table(&ctx, name);
    		printf("%s table was created.\n", name);
			grn_db_touch(&ctx, db);

    		table_num++;
    		i = 0;
    	}

    	int rd = rand();
    	record.key.start_timestamp = timestamp;
    	record.key.end_timestamp = timestamp + 20;
    	record.key.dst_host.ip_addr = (12345 << 2) + i % 1000;
    	record.key.src_host.ip_addr = (12345 << 1) + i % 1000;
    	record.key.dst_host.mac_addr = (54321 << 2) + i % 1000;
    	record.key.src_host.mac_addr = (54321 << 1) + i % 1000;
    	record.key.dst_host.port = 80;
    	record.key.src_host.port = 4000 + i % 1000;
    	record.value.protocol_id = 67;
    	record.value.request_len = 128;
    	record.value.response_len = 128;
    	record.value.return_code = 200 + rd % 200;
    	record.value.request_code = rd % 10;
    	record.value.stats.art = 0;
    	record.value.stats.eurt = rd % 100;
    	record.value.stats.in_byte = rd % 1000;
    	record.value.stats.out_byte = rd % 1000;
    	record.value.stats.nw_delay = rd % 50;
    	record.value.stats.retrans = rd % 300;
    	record.value.stats.zwin = rd % 150;

    	memset(freetext, 0, 80);
    	for(j = 0; j < 79; j++)
    	{
    		rd = rand();
    		freetext[j] = (uint8_t)(rd % (80-33) + 33);
    	}

    	sprintf(record.value.request, "GET HTTP/1.1 %s", freetext);
    	sprintf(record.value.response, "200 OK %s", freetext);
    	buf.last_timestamp = set_record_to_json(&record, &buf.array);

		if(json_array_get_count(buf.array) >= 200)
		{
			buf.serialized_string = json_serialize_to_string_pretty(buf.json_root);
			table = grn_ctx_get(&ctx, name, strlen(name));
			
			int start = clock();
			int num = insert_records(&ctx, table, buf.serialized_string, name);
			printf("insert %d records\n", num);
			int stop = clock();
			grn_obj_unlink(&ctx, table);
			json_array_clear(buf.array);
			free(buf.serialized_string);

			grn_db_touch(&ctx, db);
			usleep(100);
			printf("%lu: updated database takes %d usec.latest timestamp is %lu\n", i, stop - start, timestamp);
		}

    	timestamp++;
    	i++;
    }


	grn_obj_close(&ctx, db);
	grn_ctx_fin(&ctx);
	return 0;
}
