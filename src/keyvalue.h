#ifndef KEY_VALUE_H
#define KEY_VALUE_H

#include "utils.h"

struct KeyValue {
    char *buf;
    int bufsize, offset;
    struct KeyValue *next;
};

struct KeyMultiValue {
    char *key;
    int keysize;
    int nvals, capvals;
    char **multival;
    int *valsize;
    struct KeyMultiValue *next;
};

void KeyMultiValue_clear_node(struct KeyMultiValue *node);
void KeyMultiValue_add_value(struct KeyMultiValue *kmv, struct KeyValue *kv);
void KeyMultiValue_add_node(struct KeyMultiValue **kmv, struct KeyValue *kv);
void KeyMultiValue_add_kv(struct KeyMultiValue **kmv, struct KeyValue *kv);
void KeyValue_take_buffer(struct KeyValue **kv, char *buf, int bufsize);
void KeyValue_add_buffer(struct KeyValue **kv, char *buf, int bufsize);
void KeyValue_add(struct KeyValue **kv, char *key, int keysize, char *val, int valsize);

char *KeyValue_get_keyptr(struct KeyValue *kv);
int KeyValue_get_keysize(struct KeyValue *kv);

#endif /* KEY_VALUE_H */