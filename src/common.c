#include "common.h"

#include <stdlib.h>
#include <string.h>
#include <wchar.h>

char* createStringCopy(const char* string) {
    char *result = malloc(sizeof(char) * (strlen(string) + 1));
    strcpy(result, string);
    return result;
}

wchar_t* createLStringCopy(const wchar_t* string) {
    wchar_t *result = malloc(sizeof(wchar_t) * (wcslen(string) + 1));
    wcscpy(result, string);
    return result;
}

KeyValuePair createWordCountPair(const wchar_t *word, size_t count) {
    KeyValuePair result;

    result.key = createLStringCopy(word);
    result.value = malloc(sizeof(size_t));

    size_t *countPointer = result.value;
    *countPointer = count;

    return result;
}

Vector* createVector() {
    Vector *result = malloc(sizeof(Vector*));

    result->size = 0;
    result->capacity = 4;
    result->storage = malloc(sizeof(void*) * 4);

    return result;
}

void resizeVector(Vector *vector, size_t newCapacity) {
    vector->capacity = newCapacity;
    vector->storage = realloc(vector->storage, sizeof(void*) * newCapacity);
}

void pushBack(Vector *vector, void *element) {
    vector->storage[vector->size++] = element;

    if (vector->size * 2 > vector->capacity) {
        resizeVector(vector, vector->capacity * 2);
    }
}

int compareKeyValuePairs(const void *leftPointer, const void *rightPointer) {
    KeyValuePair *leftPair = *((KeyValuePair**)leftPointer);
    KeyValuePair *rightPair = *((KeyValuePair**)rightPointer);

    return wcscmp(leftPair->key, rightPair->key);
}