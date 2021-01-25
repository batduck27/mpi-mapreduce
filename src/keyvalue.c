#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "keyvalue.h"
#include "utils.h"

#define INIT_MULTIVALS_CAP 16

void KeyMultiValue_clear_node(struct KeyMultiValue *node)
{
    if (node == NULL) {
        return;
    }

    free(node->key);
    for (int i = 0; i < node->nvals; ++ i) {
        free(node->multival[i]);
    }
    free(node->multival);
    free(node->valsize);
    free(node);
}

void KeyMultiValue_add_value(struct KeyMultiValue *kmv, struct KeyValue *kv)
{
    struct KeyMultiValue *tmp;
    int keysize, valsize;
    char *valptr;
    int offset = 0;

    if (kmv == NULL || kv == NULL) {
        return;
    }

    memcpy(&keysize, kv->buf + offset, sizeof(keysize));
    offset += sizeof(keysize) + keysize;
    memcpy(&valsize, kv->buf + offset, sizeof(valsize));
    offset += sizeof(valsize);
    valptr = kv->buf + offset;

    if (kmv->nvals >= kmv->capvals) {
        kmv->capvals <<= 1;
        kmv->multival = realloc(kmv->multival, sizeof(*(kmv->multival)) * kmv->capvals);
        DIE(kmv->multival == NULL, "realloc failed");
        kmv->valsize = realloc(kmv->valsize, sizeof(*(kmv->valsize)) * kmv->capvals);
        DIE(kmv->valsize == NULL, "realloc failed");
    }

    kmv->valsize[kmv->nvals] = valsize;
    kmv->multival[kmv->nvals] = malloc(sizeof(**(kmv->multival)) * valsize);
    DIE(kmv->multival[kmv->nvals] == NULL, "malloc failed");

    memcpy(kmv->multival[kmv->nvals], valptr, valsize);
    kmv->nvals ++;
}

void KeyMultiValue_add_node(struct KeyMultiValue **kmv, struct KeyValue *kv)
{
    struct KeyMultiValue *tmp;
    int keysize;
    char *keyptr;

    if (kv == NULL) {
        return;
    }

    memcpy(&keysize, kv->buf, sizeof(keysize));
    keyptr = kv->buf + sizeof(keysize);

    tmp = malloc(sizeof(*tmp));
    DIE(tmp == NULL, "malloc failed");

    tmp->key = malloc(sizeof(*(tmp->key)) * keysize);
    DIE(tmp->key == NULL, "malloc failed");
    memcpy(tmp->key, keyptr, keysize);
    tmp->keysize = keysize;

    tmp->nvals = 0;
    tmp->capvals = INIT_MULTIVALS_CAP;
    tmp->multival = malloc(sizeof(*(tmp->multival)) * tmp->capvals);
    DIE(tmp->multival == NULL, "malloc failed");
    tmp->valsize = malloc(sizeof(*(tmp->valsize)) * tmp->capvals);
    DIE(tmp->valsize == NULL, "malloc failed");
    tmp->next = NULL;

    KeyMultiValue_add_value(tmp, kv);

    if (*kmv != NULL) {
        tmp->next = *kmv;
    }

    *kmv = tmp;
}

void KeyMultiValue_add_kv(struct KeyMultiValue **kmv, struct KeyValue *kv)
{
    struct KeyMultiValue *tmp;
    int keysize;
    char *keyptr;

    if (kv == NULL) {
        return;
    }

    memcpy(&keysize, kv->buf, sizeof(keysize));
    keyptr = kv->buf + sizeof(keysize);

    tmp = *kmv;
    while (tmp != NULL) {
        if (tmp->keysize == keysize && memcmp(keyptr, tmp->key, keysize) == 0) {
            KeyMultiValue_add_value(tmp, kv);
            break;
        }

        tmp = tmp->next;
    }

    if (tmp == NULL) {
        KeyMultiValue_add_node(kmv, kv);
    }
}

static void copy_in_buf(struct KeyValue *kv, char *src, int srcsize)
{
    memcpy(kv->buf + kv->offset, src, srcsize);
    kv->offset += srcsize;
}

void KeyValue_add_buffer(struct KeyValue **kv, char *buf, int bufsize)
{
    struct KeyValue *tmp = NULL;
    int offset = 0;

    tmp = malloc(sizeof(*tmp));
    DIE(tmp == NULL, "malloc failed");

    tmp->offset = 0;
    tmp->next = NULL;
    tmp->bufsize = bufsize;
    tmp->buf = malloc(sizeof(*tmp->buf) * tmp->bufsize);
    DIE(tmp->buf == NULL, "malloc failed");

    copy_in_buf(tmp, buf, bufsize);

    if (*kv != NULL) {
        tmp->next = *kv;
    }

    *kv = tmp;
}

void KeyValue_take_buffer(struct KeyValue **kv, char *buf, int bufsize)
{
    struct KeyValue *tmp = NULL;
    int offset = 0;

    tmp = malloc(sizeof(*tmp));
    DIE(tmp == NULL, "malloc failed");

    tmp->offset = 0;
    tmp->next = NULL;
    tmp->bufsize = bufsize;
    tmp->buf = buf;
    tmp->offset = bufsize;

    if (*kv != NULL) {
        tmp->next = *kv;
    }

    *kv = tmp;
}

void KeyValue_add(struct KeyValue **kv, char *key, int keysize, char *val, int valsize)
{
    struct KeyValue *tmp = NULL;
    int offset = 0;

    tmp = malloc(sizeof(*tmp));
    DIE(tmp == NULL, "malloc failed");

    tmp->offset = 0;
    tmp->next = NULL;
    tmp->bufsize = keysize * sizeof(*key) + sizeof(keysize) +
                   valsize * sizeof(*val) + sizeof(valsize);
    tmp->buf = malloc(sizeof(*tmp->buf) * tmp->bufsize);
    DIE(tmp->buf == NULL, "malloc failed");

    copy_in_buf(tmp, (char *)&keysize, sizeof(keysize));
    copy_in_buf(tmp, key, keysize);
    copy_in_buf(tmp, (char *)&valsize, sizeof(valsize));
    copy_in_buf(tmp, val, valsize);

    if (*kv != NULL) {
        tmp->next = *kv;
    }

    *kv = tmp;
}

char *KeyValue_get_keyptr(struct KeyValue *kv)
{
    if (kv == NULL || kv->buf == NULL) {
        return NULL;
    }

    return kv->buf + sizeof(int);
}

int KeyValue_get_keysize(struct KeyValue *kv)
{
    int keysize;

    if (kv == NULL || kv->buf == NULL) {
        return -1;
    }

    memcpy(&keysize, kv->buf, sizeof(keysize));

    return keysize;
}