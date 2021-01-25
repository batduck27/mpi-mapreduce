#ifndef MAP_REDUCE_H
#define MAP_REDUCE_H

#include <stdio.h>

#include "keyvalue.h"
#include "utils.h"
#include "stdint.h"

typedef void (*MapFunction)(char *filename, struct KeyValue **kv);
typedef void (*ReduceFunction)(char *key, int keysize, int nvals, char **multival, int *valsize, struct KeyValue **kv);
typedef void (*PrintFunction)(char *key, int keysize, char *val, int valsize, FILE *file);
typedef uint32_t (*HashFunction)(char *key, int keysize);

enum MapSplitType {
    CHUNK_SPLIT,
    MASTER_SLAVE,
    UNAVAILBALE
};

struct MR_context {
    MPI_Comm comm;
    int proc_rank, nprocs;
    int mapstyle;

    struct KeyValue *kv;
    struct KeyMultiValue *kmv;

    HashFunction hashfunc;
};

struct MR_context *MR_alloc();
void MR_delete(struct MR_context *ctx);
void MR_init(struct MR_context *ctx);
void MR_deinit(struct MR_context *ctx);

void MR_map(struct MR_context *ctx, MapFunction mapfunc, int nstr, char **strs);
void MR_reduce(struct MR_context *ctx, ReduceFunction reducefunc, PrintFunction printfunc, char *filename);

#endif /* MAP_REDUCE_H */