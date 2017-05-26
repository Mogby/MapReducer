#ifndef COMMON_H

#define COMMON_H

#include <stddef.h>

char* createStringCopy(const char* strign);

wchar_t* createLStringCopy(const wchar_t* string);

typedef struct _key_value_pair_struct {
    void *key;
    void *value;
} KeyValuePair;

KeyValuePair createWordCountPair(const wchar_t *word, size_t count);

typedef struct _vector_struct {
    size_t size;
    size_t capacity;
    void **storage;
} Vector;

Vector* createVector();

void pushBack(Vector *vector, void *element);

int compareKeyValuePairs(const void *leftPointer, const void *rightPointer);

void freeVector(Vector *vector);

void freeVectorOfPairs(Vector *vector);

#endif