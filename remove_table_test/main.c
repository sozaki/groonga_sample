/*
 * main.c
 *
 *  Created on: Oct 20, 2017
 *      Author: root
 */

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <groonga.h>
#include <time.h>
#include <sys/time.h>

static grn_ctx insert_ctx;
static 	grn_obj* insert_db;
struct timeval start_time;
struct timeval end_time;

int remove_grn_table(grn_ctx* ctx, char *name, int length)
{
	grn_rc result;
	grn_obj* obj = grn_ctx_get(ctx, name, length);
	char index_table[30];
	memset(index_table, 0, 30);
	if (obj == NULL) {
			printf("%s has already been removed.\n", name);
			return 1;
	}

	printf("length: %d: <%.*s>\n", length, length, name);
	sprintf(index_table, "%.*s_index", length, name);

	if(length == 13){
		result = grn_obj_remove(ctx, obj);
		grn_obj* obj_index = grn_ctx_get(ctx, index_table, strlen(index_table));
		if(obj_index != NULL)
		{
			result  = grn_obj_remove(ctx, obj_index);
		}
		if (result != GRN_SUCCESS) {
				printf("Failed to remove %s obj. : %d\n", name, result);
		} else {
				printf("%s is removed.\n", name);
		}
	}
	return result == GRN_SUCCESS;

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

	gettimeofday(&start_time, NULL);
	grn_expr_exec(ctx, command, 0);
	gettimeofday(&end_time, NULL);

	printf("%lf\n", end_time.tv_sec - start_time.tv_sec + (double)(end_time.tv_usec - start_time.tv_usec)/1000.0/1000.0);

	if(ctx->rc == GRN_CANCEL) printf("canceled command.\n");
	grn_ctx_recv(ctx, &result, &result_length, &recv_flags);
	grn_expr_clear_vars(ctx, command);
	grn_obj_unlink(ctx, command);

	char str_return[result_length+ 1];
	memset(str_return, 0, result_length + 1);
	memcpy(str_return, result, result_length);
	int num = atoi(str_return);
	return num;
}


int create_grn_table(grn_ctx* ctx, char *_table_name, char *_flags, char *_key_type, char *_normalizer, char *_tokenizer)
{
	grn_obj* key_type = grn_ctx_at(ctx, GRN_DB_UINT64);
	grn_obj* text_type = grn_ctx_at(ctx, GRN_DB_SHORT_TEXT);
	grn_obj* table = grn_table_create(ctx, _table_name, strlen(_table_name), NULL, GRN_OBJ_TABLE_HASH_KEY|GRN_OBJ_PERSISTENT, key_type, NULL);
	grn_obj* column = grn_column_create(ctx, table, "value", strlen("value"), NULL, GRN_OBJ_COLUMN_SCALAR|GRN_OBJ_PERSISTENT, text_type);
	grn_obj* column2 = grn_column_create(ctx, table, "num", strlen("num"), NULL, GRN_OBJ_COLUMN_SCALAR|GRN_OBJ_PERSISTENT, key_type);
	char index_table_name[30];
	sprintf(index_table_name, "%s_index", _table_name);
	grn_obj* tokenizer = grn_ctx_at(ctx, GRN_DB_BIGRAM);
	grn_obj* index_table = grn_table_create(ctx, index_table_name, strlen(index_table_name), NULL, GRN_OBJ_TABLE_PAT_KEY|GRN_OBJ_PERSISTENT|GRN_OBJ_KEY_NORMALIZE, text_type, NULL);
	grn_obj_set_info(ctx, index_table, GRN_INFO_DEFAULT_TOKENIZER, tokenizer);
	grn_obj* column_index = grn_column_create(ctx, index_table, "index_value", strlen("index_value"), NULL, GRN_OBJ_COLUMN_SCALAR|GRN_OBJ_PERSISTENT, column);
	grn_obj_unlink(ctx, key_type);

    if(table == NULL) printf("Failed to create\n");
    else grn_obj_unlink(ctx, table);

    if(column == NULL) printf("Failed to create column\n");
    else grn_obj_unlink(ctx, column);

    if(column2 == NULL) printf("Failed to create column2\n");
    else grn_obj_unlink(ctx, column2);

    if(index_table == NULL) printf("Failed to create\n");
    else grn_obj_unlink(ctx, index_table);

    if(column_index == NULL) printf("Failed to create index\n");
    else grn_obj_unlink(ctx, column_index);
    return 1;
}

void create_and_remove_table()
{
	timer_t timer;
	char name[15];
	int i = 0;
	int j = 0;
	char record[100000];
	grn_obj* obj;
	char table_name[30];
	int index = 0;

	while(1)
	{
		timer = time(NULL);
		memset(name, 0, 15);
		sprintf(name, "%ld", (unsigned long long)timer * 1000 + j);
		int res = create_grn_table(&insert_ctx, name, "TABLE_HASH_KEY", "UInt64", "", "");
		printf("create %s. result = %d\n", name, res);
		for(i = 0; i < 10; i++)
		{
			memset(record, 0, 1000);
			sprintf(record, "[");
			for(j = 0; j < 1000; j++){
				sprintf(record + strlen(record), "{\"_key\":\"%ld\", \"value\":%s, \"num\":%ld},", (unsigned long long)timer * 1000000000 + i, "value", (unsigned long long)timer * 1000 + 2 * i);
			}
			sprintf(record + strlen(record), "]");
			load_data(&insert_ctx, insert_db, record, name);
		}
		grn_db_touch(&insert_ctx, insert_db);
		printf("set records\n");
		sleep(2);

		grn_obj tables;
		GRN_PTR_INIT(&tables, GRN_OBJ_VECTOR, GRN_ID_NIL);
		grn_rc result = grn_ctx_get_all_tables(&insert_ctx, &tables);
		if(result == GRN_SUCCESS){
			j = GRN_BULK_VSIZE(&tables) / sizeof(grn_obj *);
			printf("%d tables exists\n", j);
		}

		if(j > 15){
			int length = grn_obj_name(&insert_ctx, GRN_PTR_VALUE_AT(&tables, 0), table_name, 30);
			obj = grn_ctx_get(&insert_ctx, table_name, length);
			remove_grn_table(&insert_ctx, table_name, length);
			grn_db_unmap(&insert_ctx, insert_db);
			if(index > 10) system("service groonga-httpd restart");
			index++;
		}
		grn_obj_close(&insert_ctx, &tables);

		sleep(1);
	}

}

int main(int argc, char** argv)
{

	grn_rc result = grn_set_lock_timeout(100);
	printf("default log_path = %s\n", grn_default_logger_get_path());

	grn_set_default_request_timeout(0.001);
	double timeout = grn_get_default_request_timeout();

	grn_init();
	grn_ctx_init(&insert_ctx, 0);
	result = grn_ctx_set_output_type(&insert_ctx, GRN_CONTENT_JSON);
	insert_db = grn_db_open(&insert_ctx, "/tmp/uila/db/uila.db");

	if(result != GRN_SUCCESS)
	{
		return -1;
	}

	create_and_remove_table();

}
