#include "communication.h"
#include "common.h"

#include <unistd.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

const uint16_t MASTER_PORT_NUMBER = 8804;
const uint16_t REDUCER_PORT_NUMBER = 8805;

const u_char COMMAND_REGISTER_MAPPER = 0;
const u_char COMMAND_REGISTER_REDUCER = 1;
const u_char COMMAND_STOP = 2;
const u_char COMMAND_HANDLE_KEYVALUE = 3;
const u_char COMMAND_GET_REDUCER = 4;

void networkRead(int socket, void *buffer, size_t size) {
    read(socket, buffer, size);

    if (size == 2) {
        uint16_t *dataPointer = (uint16_t*)buffer;
        *dataPointer = ntohs(*dataPointer);
    } else if (size == 4) {
        uint32_t *dataPointer = (uint32_t*)buffer;
        *dataPointer = ntohl(*dataPointer);
    } else if (size == 8) {
        if (htons(1) != 1) {
            char *data = (char*)buffer;
            char temporary;
            for (size_t index = 0; index < 4; ++index) {
                temporary = data[index];
                data[index] = data[7 - index];
                data[7 - index] = temporary;
            }
        }
    }
}

void networkWrite(int socket, const void *buffer, size_t size) {
    if (size == 2) {
        uint16_t data = *(uint16_t*)buffer;
        data = htons(data);
        write(socket, &data, sizeof(data));
    } else if (size == 4) {
        uint32_t data = *(uint32_t*)buffer;
        data = htonl(data);
        write(socket, &data, sizeof(data));
    } else if (size == 8) {
        if (htons(1) != 1) {
            char *data = malloc(8);
            for (size_t index = 0; index < 8; ++index) {
                data[index] = ((char *) buffer)[7 - index];
            }
            write(socket, data, 8);
            free(data);
        } else {
            write(socket, buffer, size);
        }
    } else {
        write(socket, buffer, size);
    }
}

ServerInfo startServer(uint16_t portNumber) {
    ServerInfo result;

    result.serverSocket = socket(AF_INET, SOCK_STREAM, 0);
    if (result.serverSocket < 0) {
        perror("ERROR opening socket");
        exit(1);
    }

    puts("Socket opened");

    struct sockaddr_in serverAddress;
    bzero(&serverAddress, sizeof(serverAddress));

    serverAddress.sin_family = AF_INET;
    serverAddress.sin_addr.s_addr = INADDR_ANY;
    serverAddress.sin_port = htons(portNumber);

    if (bind(result.serverSocket, (struct sockaddr*)&serverAddress, sizeof(serverAddress)) < 0) {
        perror("ERROR binding socket");
        exit(1);
    }

    puts("Socket bound");

    result.mappers = createVector();
    result.reducers = createVector();
    sem_init(&result.semaphore, 0, 0);
    pthread_mutex_init(&result.mutex, NULL);

    return result;
}

void shutdownServer(ServerInfo server) {
    freeVector(server.mappers);
    freeVector(server.reducers);
    pthread_mutex_destroy(&server.mutex);
    sem_destroy(&server.semaphore);
    close(server.serverSocket);
}

int getReducerForKey(const char *key, Vector *reducers, size_t *reducerIndex) {
    size_t leftBound = 0;
    size_t rightBound = reducers->size;
    size_t middleIndex;

    while (leftBound < rightBound) {
        middleIndex = (leftBound + rightBound) / 2;

        if (strcmp(((KeyValuePair*)reducers->storage[middleIndex])->key, key) < 0) {
            leftBound = middleIndex + 1;
        } else {
            rightBound = middleIndex;
        }
    }

    if (leftBound == reducers->size || strcmp(((KeyValuePair*)reducers->storage[leftBound])->key, key)) {
        return -1;
    }

    *reducerIndex = *(size_t*)((KeyValuePair*)reducers->storage[leftBound])->value;

    return 0;
}

void addReducer(const char *key, Vector *reducers, size_t reducerIndex) {
    size_t insertionIndex = 0;
    while (insertionIndex < reducers->size &&
            strcmp(((KeyValuePair*)reducers->storage[insertionIndex])->key, key) < 0) {
        ++insertionIndex;
    }

    pushBack(reducers, NULL);
    for (size_t index = reducers->size - 1; index > insertionIndex; --index) {
        reducers->storage[index] = reducers->storage[index - 1];
    }

    KeyValuePair *newElement = malloc(sizeof(KeyValuePair));
    newElement->key = createStringCopy(key);
    newElement->value = malloc(sizeof(size_t));
    *((size_t*)newElement->value) = reducerIndex;

    reducers->storage[insertionIndex] = newElement;
}

