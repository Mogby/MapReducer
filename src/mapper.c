#include "common.h"

#include <dirent.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <wctype.h>
#include <wchar.h>
#include <locale.h>

static FILE *logFile;

Vector* map(KeyValuePair file) {
    size_t bufferLength;
    wchar_t buffer[4096];

    wchar_t *leftBound = file.value;
    wchar_t *rightBound;

    Vector *result = createVector();
    while (*leftBound) {
        while (*leftBound && !iswalnum(*leftBound))
            ++leftBound;

        bufferLength = 0;
        rightBound = leftBound;
        while (*rightBound && iswalnum(*rightBound))
            buffer[bufferLength++] = towlower(*(rightBound++));
        buffer[bufferLength] = 0;

        if (bufferLength) {
            KeyValuePair *newElement = malloc(sizeof(KeyValuePair));
            *newElement = createWordCountPair(buffer, 1);
            pushBack(result, (void*)newElement);
        }

        leftBound = rightBound;
    }

    return result;
}

wchar_t* readWholeFile(const char *filename) {
    FILE *file = fopen(filename, "r");

    size_t bufferLength = 0;
    size_t bufferCapacity = 4;
    wchar_t *buffer = malloc(sizeof(wchar_t) * 4);

    wint_t nextChar;
    while ((nextChar = fgetwc(file)) != WEOF) {
        buffer[bufferLength++] = nextChar;
        if (bufferLength * 2 >= bufferCapacity) {
            bufferCapacity *= 2;
            buffer = realloc(buffer, sizeof(wchar_t) * bufferCapacity);
        }
    }

    buffer = realloc(buffer, sizeof(wchar_t) * (bufferLength + 1));
    buffer[bufferLength] = 0;

    fclose(file);

    return buffer;
}

void handleFile(const char *filename) {
    KeyValuePair fileDescription;
    fileDescription.key = createStringCopy(filename);
    fileDescription.value = readWholeFile(filename);

    Vector *wordsList = map(fileDescription);
    qsort(wordsList->storage, wordsList->size, sizeof(void*), compareKeyValuePairs);
    size_t leftBound = 0, rightBound = 0;
    size_t wordCount;
    while (leftBound < wordsList->size) {
        wordCount = 0;
        while (rightBound < wordsList->size &&
                !compareKeyValuePairs(wordsList->storage + leftBound,
                                      wordsList->storage + rightBound)) {
            wordCount += *(size_t*)((KeyValuePair*)wordsList->storage[rightBound])->value;
            ++rightBound;
        }
        wprintf(L"%ls : %lu\n", ((KeyValuePair*)wordsList->storage[leftBound])->key, wordCount);

        leftBound = rightBound;
    }

    freeVectorOfPairs(wordsList);
    free(fileDescription.key);
    free(fileDescription.value);
}

void processInput() {
    DIR *directory = opendir("./data");
    struct dirent *directoryEntry;

    if (!directory) {
        perror("ERROR opening ./data directory");
        exit(EXIT_FAILURE);
    }

    char filename[1024];
    while (directoryEntry = readdir(directory)) {
        if (directoryEntry->d_type == DT_REG) {
            strcpy(filename, "./data/");
            strcat(filename, directoryEntry->d_name);
            fprintf(logFile, "Handling ./data/%s\n", directoryEntry->d_name);
            handleFile(filename);
        } else {
            fprintf(logFile, "Ignoring ./data/%s\n", directoryEntry->d_name);
        }
    }

    closedir(directory);
}

int main() {
    setlocale(LC_CTYPE, "");

    logFile = fopen("mapper.log", "w");

    processInput();

    fclose(logFile);

    return EXIT_SUCCESS;
}