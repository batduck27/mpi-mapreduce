#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "sys/types.h"
#include "sys/stat.h"
#include "dirent.h"

#include "mpi.h"

#include "mapreduce.h"
#include "hash.h"

#define MASTER_NODE 0
#define INIT_FILES_CAP 16

#define MIN(a, b) ((a < b) ? (a) : (b))

static void add_to_files(char *path, char ***files, int *nfiles, int *capfiles)
{
    char *tmp;

    if (*nfiles >= *capfiles) {
        *capfiles <<= 1;
        *files = realloc(*files, sizeof(**files) * (*capfiles));
        DIE(*files == NULL, "Failed to alloc memory for files list");
    }

    tmp = strdup(path);
    DIE(tmp == NULL, "strdup failed");

    (*files)[(*nfiles)++] = tmp;
}

static void get_files(char *path, char ***files, int *nfiles, int *capfiles)
{
    int err;
    struct stat buf;

    err = stat(path, &buf);
    if (err) {
        fprintf(stderr, "[WARNING] Couldn't query status for path %s\n", path);
    } else if (S_ISREG(buf.st_mode)) {
        add_to_files(path, files, nfiles, capfiles);
    } else if (S_ISDIR(buf.st_mode)) {
        struct dirent *ep;
        DIR *dp = opendir(path);

        if (dp == NULL) {
            fprintf(stderr, "[WARNING] Couldn't query status for path %s\n", path);
            return;
        }

        while (ep = readdir(dp)) {
            char new_path[512];
            if (ep->d_name[0] == '.') {
                continue;
            }

            sprintf(new_path,"%s/%s", path, ep->d_name);
            get_files(new_path, files, nfiles, capfiles);
        }

        closedir(dp);
    } else {
        fprintf(stderr, "[WARNING] Invalid path %s for map\n", path);
    }
}

static void broadcast_file_list(struct MR_context *ctx, char ***files, int *nfiles)
{
    MPI_Bcast(nfiles, 1, MPI_INT, 0, ctx->comm);

    if (ctx->proc_rank != MASTER_NODE) {
        *files = malloc(sizeof(**files) * (*nfiles));
        DIE(*files == NULL, "Failed to alloc memory for files list");
    }

    for (int i = 0; i < *nfiles; ++ i) {
        int filename_length;

        if (ctx->proc_rank == MASTER_NODE) {
            filename_length = strlen((*files)[i]) + 1;
        }

        MPI_Bcast(&filename_length, 1, MPI_INT, 0, ctx->comm);

        if (ctx->proc_rank != MASTER_NODE) {
            (*files)[i] = malloc(sizeof(***files) * filename_length);
            DIE((*files)[i] == NULL, "malloc failed");
        }

        MPI_Bcast((*files)[i], filename_length, MPI_CHAR, 0, ctx->comm);
    }
}

struct MR_context *MR_alloc()
{
    struct MR_context *tmp;

    tmp = malloc(sizeof(struct MR_context));
    DIE(tmp == NULL, "Failed to alloc memory for map reduce context");

    return tmp;
}

void MR_delete(struct MR_context *ctx)
{
    if (ctx == NULL) {
        return;
    }

    free(ctx);
}

void MR_init(struct MR_context *ctx)
{
    int flag;

    if (ctx == NULL) {
        fprintf(stderr, "[ERROR] invalid MR context\n");
        exit(EINVAL);
    }

    MPI_Initialized(&flag);

    if (!flag) {
        int argc = 0;
        char **argv = NULL;

        MPI_Init(&argc, &argv);
    }

    ctx->comm = MPI_COMM_WORLD;
    MPI_Comm_rank(ctx->comm, &ctx->proc_rank);
    MPI_Comm_size(ctx->comm, &ctx->nprocs);

    ctx->hashfunc = NULL;
    ctx->mapstyle = CHUNK_SPLIT;
    ctx->kv = NULL;
    ctx->kmv = NULL;
}

void MR_deinit(struct MR_context *ctx)
{
    int flag;

    if (ctx == NULL) {
        return;
    }

    MPI_Initialized(&flag);

    if (flag) {
        MPI_Finalize();
    }

    // TODO
}

void MR_map(struct MR_context *ctx, MapFunction mapfunc, int nstr, char **strs)
{
    char **files = NULL;
    int nfiles = 0, capfiles = INIT_FILES_CAP;
    MPI_Status status;

    if (ctx->proc_rank == MASTER_NODE) {
        files = malloc(sizeof(*files) * capfiles);
        DIE(files == NULL, "Failed to alloc memory for files list");

        for (int i = 0; i < nstr; ++ i) {
            get_files(strs[i], &files, &nfiles, &capfiles);
        }
    }

    broadcast_file_list(ctx, &files, &nfiles);

    if (ctx->nprocs == 1 || ctx->mapstyle == CHUNK_SPLIT) {
        int start = ctx->proc_rank * nfiles / ctx->nprocs;
        int end = MIN((ctx->proc_rank + 1) * nfiles / ctx->nprocs, nfiles);

        for (int i = start; i < end; ++ i) {
            mapfunc(files[i], &(ctx->kv));
        }
    } else if (ctx->mapstyle == MASTER_SLAVE) {
        if (ctx->proc_rank == 0) {
            const int doneflag = -1;
            int ndone = 0, itask = 0;

            for (int i = MASTER_NODE + 1; i < ctx->nprocs; ++ i) {
                if (itask < nfiles) {
                    MPI_Send(&itask, 1, MPI_INT, i, 0, ctx->comm);
                    itask ++;
                } else {
                    MPI_Send(&doneflag, 1, MPI_INT, i, 0, ctx->comm);
                    ndone ++;
                }
            }

            while (ndone < ctx->nprocs - 1) {
                int tmp, i;

                MPI_Recv(&tmp, 1, MPI_INT, MPI_ANY_SOURCE, 0, ctx->comm, &status);
                i = status.MPI_SOURCE;

                if (itask < nfiles) {
                    MPI_Send(&itask, 1, MPI_INT, i, 0, ctx->comm);
                    itask ++;
                } else {
                    MPI_Send(&doneflag, 1, MPI_INT, i, 0, ctx->comm);
                    ndone ++;
                }
            }
        } else {
            int doneflag = 0;

            while (!doneflag) {
                int tmp;

                MPI_Recv(&tmp, 1, MPI_INT, MASTER_NODE, 0, ctx->comm, &status);
                if (tmp >= 0) {
                    mapfunc(files[tmp], &(ctx->kv));
                    MPI_Send(&tmp, 1, MPI_INT, MASTER_NODE, 0, ctx->comm);
                } else {
                    doneflag = 1;
                }
            }
        }
    } else {
        fprintf(stderr, "[ERROR] Invalid mapstyle: %d\n", ctx->mapstyle);
        exit(EINVAL);
    }

    for (int i = 0; i < nfiles; ++ i) {
        free(files[i]);
    }

    free(files);

    MPI_Barrier(ctx->comm);
}

static void shuffle(struct MR_context *ctx)
{
    MPI_Status status;

    if (ctx->nprocs == 1) {
        struct KeyValue *tmp_kv;

        while (ctx->kv != NULL) {
            KeyMultiValue_add_kv(&ctx->kmv, ctx->kv);

            tmp_kv = ctx->kv;
            ctx->kv = ctx->kv->next;

            free(tmp_kv->buf);
            free(tmp_kv);
        }
    } else {
        for (int i = 0; i < ctx->nprocs; ++ i) {
            if (i == ctx->proc_rank) {
                int doneflag = -1;

                while (ctx->kv != NULL) {
                    char *key;
                    int keysize;
                    uint32_t hash;
                    struct KeyValue *tmp_kv;

                    key = KeyValue_get_keyptr(ctx->kv);
                    DIE(key == NULL, "get_keyptr failed");
                    keysize = KeyValue_get_keysize(ctx->kv);
                    DIE(keysize < 0, "get_keysize failed");

                    if (ctx->hashfunc != NULL) {
                        hash = ctx->hashfunc(key, keysize);
                    } else {
                        hash = hashlittle(key, keysize, (uint32_t)ctx->nprocs) % (uint32_t)ctx->nprocs;
                    }

                    if (hash == ctx->proc_rank) {
                        KeyMultiValue_add_kv(&ctx->kmv, ctx->kv);
                    } else {
                        MPI_Send(&(ctx->kv->bufsize), 1, MPI_INT, hash, 0, ctx->comm);
                        MPI_Send(ctx->kv->buf, ctx->kv->bufsize, MPI_CHAR, hash, 0, ctx->comm);
                    }

                    tmp_kv = ctx->kv;
                    ctx->kv = ctx->kv->next;

                    free(tmp_kv->buf);
                    free(tmp_kv);
                }

                for (int iproc = 0; iproc < ctx->nprocs; ++ iproc) {
                    if (iproc != ctx->proc_rank) {
                        MPI_Send(&doneflag, 1, MPI_INT, iproc, 0, ctx->comm);
                    }
                }
            } else {
                int doneflag = 0;

                while (!doneflag) {
                    int recval, sender;

                    MPI_Recv(&recval, 1, MPI_INT, MPI_ANY_SOURCE, 0, ctx->comm, &status);
                    sender = status.MPI_SOURCE;

                    if (recval >= 0) {
                        struct KeyValue tmp_kv;

                        tmp_kv.buf = malloc(sizeof(*(tmp_kv.buf)) * recval);
                        DIE(tmp_kv.buf == NULL, "malloc failed");

                        MPI_Recv(tmp_kv.buf, recval, MPI_CHAR, sender, 0, ctx->comm, &status);
                        KeyMultiValue_add_kv(&ctx->kmv, &tmp_kv);

                        free(tmp_kv.buf);
                    } else if (sender == i) {
                        doneflag = 1;
                    } else {
                        fprintf(stderr, "[ERROR] Invalid sender %d for value %d\n", sender, recval);
                        exit(EINVAL);
                    }
                }
            }

            MPI_Barrier(MPI_COMM_WORLD);
        }
    }
}

static void merger(struct MR_context *ctx)
{
    MPI_Status status;

    if (ctx->proc_rank == MASTER_NODE) {
        int ndone = 0;

        while (ndone < ctx->nprocs - 1) {
            int recval, sender;

            MPI_Recv(&recval, 1, MPI_INT, MPI_ANY_SOURCE, 0, ctx->comm, &status);
            sender = status.MPI_SOURCE;

            if (recval >= 0) {
                char *buf;

                buf = malloc(sizeof(*buf) * recval);
                DIE(buf == NULL, "malloc failed");

                MPI_Recv(buf, recval, MPI_CHAR, sender, 0, ctx->comm, &status);
                KeyValue_take_buffer(&(ctx->kv), buf, recval);
            } else {
                ndone ++;
            }
        }
    } else {
        struct KeyValue *tmp_kv;
        int doneflag = -1;

        while (ctx->kv != NULL) {
            MPI_Send(&(ctx->kv->bufsize), 1, MPI_INT, MASTER_NODE, 0, ctx->comm);
            MPI_Send(ctx->kv->buf, ctx->kv->bufsize, MPI_CHAR, MASTER_NODE, 0, ctx->comm);

            tmp_kv = ctx->kv;
            ctx->kv = ctx->kv->next;

            free(tmp_kv->buf);
            free(tmp_kv);
        }

        MPI_Send(&doneflag, 1, MPI_INT, MASTER_NODE, 0, ctx->comm);
    }

    MPI_Barrier(MPI_COMM_WORLD);
}

void MR_reduce(struct MR_context *ctx, ReduceFunction reducefunc, PrintFunction printfunc, char *filename)
{
    struct KeyMultiValue *tmp_kmv;
    struct KeyValue *tmp_kv;
    struct KeyValue *reduce_kv;

    shuffle(ctx);

    tmp_kmv = NULL;
    reduce_kv = NULL;

    while (ctx->kmv != NULL) {
        reducefunc(ctx->kmv->key, ctx->kmv->keysize,
                     ctx->kmv->nvals, ctx->kmv->multival, ctx->kmv->valsize, &reduce_kv);

        tmp_kmv = ctx->kmv;
        ctx->kmv = tmp_kmv->next;
        KeyMultiValue_clear_node(tmp_kmv);
    }

    ctx->kv = reduce_kv;

    merger(ctx);

    if (printfunc != NULL && ctx->proc_rank == MASTER_NODE) {
        FILE *fout = stdout;

        if (filename != NULL) {
            fout = fopen(filename, "w");
            DIE(fout == NULL, "fopen failed");
        }

        while (ctx->kv != NULL) {
            int keysize, valsize;
            char *keyptr, *valptr;
            int offset = 0;

            memcpy(&keysize, ctx->kv->buf + offset, sizeof(keysize));
            offset += sizeof(keysize);
            keyptr = ctx->kv->buf + offset;
            offset += keysize;
            memcpy(&valsize, ctx->kv->buf + offset, sizeof(valsize));
            offset += sizeof(valsize);
            valptr = ctx->kv->buf + offset;

            printfunc(keyptr, keysize, valptr, valsize, fout);

            tmp_kv = ctx->kv;
            ctx->kv = ctx->kv->next;

            free(tmp_kv->buf);
            free(tmp_kv);
        }
    }
}
