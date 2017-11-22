/*
 * main.c
 *
 *  Created on: Oct 28, 2017
 *      Author: root
 */
#include <stdlib.h>
#include <stdio.h>
#include <groonga/groonga.h>
#include <pthread.h>
#include <time.h>

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
	if(num == 0)
	{
		printf("Failed to insert\n");
	}
	return num;
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
	grn_expr_clear_vars(ctx, command);
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

int remove_grn_table(grn_ctx* ctx, char* table_name)
{
	grn_obj *table = grn_ctx_get(ctx, table_name, strlen(table_name));
	if(table != NULL)
	{
		grn_rc result = grn_obj_remove(ctx, table);
		grn_obj_unlink(ctx, table);
		printf("remove table: %d\n", result);
		return result == GRN_SUCCESS;
	}

	return 0;
}


void *grn_insert(void* arg)
{
	int i = 0, j = 0, k = 0, l = 0;
	char name[5];
	memset(name, 0, 5);
	char record[100000];
	char request[50];
	char response[50];

	for(i = 0; i < 500; i++)
	{
		sprintf(name, "%03d", i);
		create_record_table(ctx_insert, name);
		printf("create new table %s", name);
		grn_obj_close(ctx_insert, db_insert);
		GRN_DB_OPEN_OR_CREATE(ctx_insert, "test/test.db", 0, db_insert);
		while(j < 150000)
		{
			memset(record, 0, 100 * 1000);
			sprintf(record, "[\n");
			for(k = 0; k < 500; k++)
			{
				memset(request, 0, 50);
				memset(response, 0, 50);
				for(l = 0; l < 50; l++)
				{
					request[l] = (uint8_t)(rand() % (122-97) + 97);
					response[l] = (uint8_t)(rand() % (122-97) + 97);
				}
				sprintf(record + strlen(record) , "{\"_key\":%lld,\"request\":\"%s\",\"response\":\"%s\"},\n", (unsigned long long)i * 1000000000 + j + k, request, response);
			}
			sprintf(record + strlen(record) , "]\n");
			//printf("%s\n", record);
			int num = load_data(ctx_insert, db_insert, record, name);
			usleep(100);
			printf("%d\n", num);
			j+=num;
		}
		printf("load all data.\n");
		j = 0;

	}
	run = 0;
}

void *grn_remove(void* arg)
{
	int i = 0, num = 0, j = 0;
	char name[5];
	memset(name, 0, 5);
	grn_rc result;
	while(run){
		for(i = 0; i < 100; i++)
		{
			sprintf(name, "%03d", i);
			grn_obj* obj = grn_ctx_get(ctx_remove, name, strlen(name));
			if(obj != NULL)
			{
				result = grn_obj_close(ctx_remove, obj);
				if(result != GRN_SUCCESS)
				{
					printf("%s table is failed.\n", name);
				}
			}
			grn_obj_unlink(ctx_remove, obj);
			usleep(100);
		}
		sleep(10);
		printf("all tables are closed\n");
		sprintf(name, "%03d", num);
		remove_grn_table(ctx_remove, name);
		num = (num + 1) % 100;
		printf("remove %s\n", name);
		j = 0;
		grn_obj_close(ctx_remove, db_remove);
		GRN_DB_OPEN_OR_CREATE(ctx_remove, "test/test.db", 0, db_remove);
	}
}

int main(void)
{
	grn_rc result = grn_set_lock_timeout(100);

	grn_init();
	ctx_insert = grn_ctx_open(0);
	grn_ctx_init(ctx_insert, 0);
	GRN_DB_OPEN_OR_CREATE(ctx_insert, "test/test.db", 0, db_insert);
	grn_ctx_set_output_type(ctx_insert, GRN_CONTENT_JSON);

	ctx_remove = grn_ctx_open(0);
	grn_ctx_init(ctx_remove, 0);
	GRN_DB_OPEN_OR_CREATE(ctx_remove, "test/test.db", 0, db_remove);

	run = 1;
	pthread_t th_insert, th_remove;
	pthread_create(&th_insert, NULL, grn_insert, NULL);
	pthread_create(&th_remove, NULL, grn_remove, NULL);
	pthread_join(th_insert, NULL);
	pthread_join(th_remove, NULL);

	grn_obj_close(ctx_insert, db_insert);
	grn_obj_close(ctx_remove, db_remove);
	grn_ctx_fin(ctx_insert);
	grn_ctx_fin(ctx_remove);
	grn_fin();
	return 0;
}
