#include "common.h"

#include <stdlib.h>
#include <stdio.h>
#include <wchar.h>
#include <locale.h>

int main() {
    freopen("reducer.out", "w", stdout);

    setlocale(LC_CTYPE, "");

    wchar_t word[1024];
    size_t count;

    Vector *wordsVector = createVector();
    while (wscanf(L"%ls : %lu", word, &count) == 2) {
        KeyValuePair *newPair = malloc(sizeof(KeyValuePair));
        *newPair = createWordCountPair(word, count);
        pushBack(wordsVector, newPair);
    }

    qsort(wordsVector->storage, wordsVector->size, sizeof(void*), compareKeyValuePairs);

    size_t leftBound = 0, rightBound = 0;
    unsigned long long totalCount;
    while (leftBound < wordsVector->size) {
        totalCount = 0;
        while (rightBound < wordsVector->size &&
                !compareKeyValuePairs(wordsVector->storage + leftBound, wordsVector->storage + rightBound)) {
            totalCount += *(size_t*)(((KeyValuePair*)wordsVector->storage[rightBound])->value);
            ++rightBound;
        }

        wprintf(L"%ls : %llu\n", ((KeyValuePair*)wordsVector->storage[leftBound])->key, totalCount);

        leftBound = rightBound;
    }

    freeVectorOfPairs(wordsVector);

    return EXIT_SUCCESS;
}