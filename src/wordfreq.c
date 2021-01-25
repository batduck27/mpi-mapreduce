#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "mpi.h"

#include "mapreduce.h"
#include "keyvalue.h"
#include "utils.h"

static const char *separators = " \\|{}[]:\";',./<>?!@#$%^&*()_+-=`~\t\n\f\r\0";

void read_files(char *filename, struct KeyValue **kv)
{
    const int filename_length = strlen(filename) + 1;
    FILE *file;
    char *buf;
    char *saveptr;
    int filesize, bytes_read;

    file = fopen(filename, "r");
    DIE(file == NULL, "fopen failed");

    fseek(file, 0L, SEEK_END);
    filesize = ftell(file) + 1;
    fseek(file, 0L, SEEK_SET);

    buf = malloc(sizeof(*buf) * filesize);
    DIE(buf == NULL, "malloc failed");

    bytes_read = fread(buf, 1, filesize, file);
    buf[bytes_read] = 0;

    fclose(file);

    char *token = strtok_r(buf, separators, &saveptr);
    while (token) {
        KeyValue_add(kv, token, strlen(token) + 1, filename, filename_length);
        token = strtok_r(NULL, separators, &saveptr);
    }

    free(buf);
}

void count_words(char *key, int keysize, int nvals, char **multival, int *valsize, struct KeyValue **kv)
{
    char buf[512] = {0};
    int resultcap = 512, resultsize = 0;
    char *result;
    int *vis;
    int multiple_vals = 0;

    vis = calloc(nvals, sizeof(*vis));
    DIE(vis == NULL, "calloc failed");

    result = malloc(sizeof(*result) * resultcap);
    DIE(result == NULL, "malloc failed");
    *result = '\0';

    for (int i = 0; i < nvals; ++ i) {
        if (vis[i] == 0) {
            int cnt = 1;
            int entry_size = 0;

            for (int j = i + 1; j < nvals; ++ j) {
                if (vis[j] != 0) {
                    continue;
                }

                if (valsize[i] == valsize[j] && memcpy(multival[i], multival[j], valsize[i])){
                    cnt ++;
                    vis[j] = 1;
                }
            }

            if (multiple_vals > 0) {
                snprintf(buf, sizeof(buf) - 1, ", %s: %d", multival[i], cnt);
            } else {
                snprintf(buf, sizeof(buf) - 1, "%s: %d", multival[i], cnt);
            }

            entry_size = strlen(buf);
            if (entry_size + resultsize + 1 > resultcap) {
                resultcap <<= 1;
                result = realloc(result, resultcap * sizeof(*result));
                DIE(result == NULL, "realloc failed");
            }

            resultsize += entry_size;
            strcat(result, buf);

            multiple_vals ++;
        }

        vis[i] = 1;
    }

    KeyValue_add(kv, key, keysize, result, resultsize + 1);
}

void print_result(char *key, int keysize, char *val, int valsize, FILE *file)
{
    fprintf(file, "%s: {%s}\n", key, val);
}

int main(int argc, char *argv[])
{
    struct MR_context ctx;

    MR_init(&ctx);
    // ctx.mapstyle = MASTER_SLAVE;

    MR_map(&ctx, read_files, argc - 1, &argv[1]);
    MR_reduce(&ctx, count_words, print_result, NULL);

    MR_deinit(&ctx);

    return 0;
}
