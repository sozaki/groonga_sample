/*
 * main.c
 *
 *  Created on: Feb 22, 2018
 *      Author: root
 */


#include <stdio.h>
#include <stdlib.h>
#include <groonga/groonga.h>
#include <time.h>
#include <sys/stat.h>
#include <time.h>
#include <sys/sysinfo.h>
#include <sys/time.h>
#include <sys/resource.h>

#define PATH "/opt/db/test.db"
#define DIR "/opt/db"
grn_ctx* ctx;
grn_obj* db;

void check_memory_usage(char* message) {
	struct rusage r;
	getrusage(RUSAGE_SELF, &r);
	printf("[grng]%s:Max memory usage was %lf MB\n", message, (double) r.ru_maxrss / 1000.0);
}

int exec_command_and_get_return(char* command) {
        FILE *fp;
        char buf[10];
//        printf("[grng] execute %s\n", command);
        if ((fp = popen(command, "r")) == NULL)
        {
                printf("[grng]Failed to execute command\n");
                return 0;
        }

        int fd = 0;

        while (fgets(buf, 10, fp) != NULL)
        {
                fd = atoi(buf);
        }
        pclose(fp);
        return fd;
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

grn_rc delete_grn_object(grn_ctx* ctx, char* name) {
        grn_rc result;
        grn_obj* db = grn_ctx_db(ctx);
        grn_obj* obj = grn_ctx_get(ctx, name, strlen(name));
        if (obj == NULL && ctx->rc == GRN_SUCCESS)
        {
			return GRN_SUCCESS;
        }
        else if (obj == NULL && ctx->rc != GRN_SUCCESS)
        {
			return ctx->rc;
        }
		grn_obj_unlink(ctx, obj);

		result = grn_obj_remove_force(ctx, name, strlen(name));

		return result;
}

void initialize(grn_ctx* _ctx)
{
	ctx = grn_ctx_open(0);
	grn_rc result = grn_ctx_init(_ctx, 0);
}

void groonga_open(grn_ctx* ctx, grn_obj *db, const char* path)
{
	GRN_DB_OPEN_OR_CREATE(ctx, PATH, 0, db);
	db = grn_db_open(ctx, path);
	grn_rc result = grn_ctx_set_output_type(ctx, GRN_CONTENT_JSON);
}

void groonga_close(grn_ctx* ctx, grn_obj* db)
{
	grn_obj_close(ctx, db);
}



int main(void)
{
	grn_init();
	initialize(&ctx);
	groonga_open(&ctx, db, PATH);
	db = grn_ctx_db(&ctx);

	int i = 0;
	char table_name[5];
	int wc = 0, files = 0;
	int pid = (int)getpid();
	char wc_command[100];
	memset(wc_command, 0, 100);
	sprintf(wc_command, "ls /proc/%d/fd | wc -l", pid);

	char exist_files_command[100];
	memset(exist_files_command, 0, 100);
	sprintf(exist_files_command, "ls %s | wc -l", DIR);
	printf(" =============== start create ================ \n");

	for(i = 1; i < 100; i++)
	{
		memset(table_name, 0, 5);
		sprintf(table_name, "%04d", i);
		if(grn_ctx_get(ctx, table_name, strlen(table_name)) != NULL) continue;
		int res = create_grn_table(&ctx, table_name, "TABLE_HASH_KEY", "UInt64", "", "");
		wc = exec_command_and_get_return(wc_command);
		files = exec_command_and_get_return(exist_files_command);
		check_memory_usage("create table");
		printf("%d files exists (open: %d). create %s table: %d\n", files, wc, table_name, res);
		usleep(100);
		db = grn_ctx_db(&ctx);
		grn_db_unmap(&ctx, db);
	}

	sleep(10);
	printf(" =============== start delete ================ \n");

	for(i = 1; i < 100; i++)
	{
		memset(table_name, 0, 5);
		sprintf(table_name, "%04d", i);
		delete_grn_object(&ctx, table_name);
//		groonga_close(&ctx, db);
//		groonga_open(&ctx, db, PATH);
//		db = grn_ctx_db(&ctx);
//		grn_db_unmap(&ctx, db);
		wc = exec_command_and_get_return(wc_command);
		files = exec_command_and_get_return(exist_files_command);
		check_memory_usage("delete table");
		printf("%d files exists (open: %d)\n", files, wc);
		usleep(100);
	}

}
