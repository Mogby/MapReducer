#ifndef COMMUNICATION_H

#define COMMUNICATION_H

#include <stdint.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <semaphore.h>

const uint16_t MASTER_PORT_NUMBER;
const uint16_t REDUCER_PORT_NUMBER;

const u_char COMMAND_REGISTER_MAPPER;
const u_char COMMAND_REGISTER_REDUCER;
const u_char COMMAND_STOP;
const u_char COMMAND_HANDLE_KEYVALUE;
const u_char COMMAND_GET_REDUCER;

void networkRead(int socket, void *buffer, size_t size);

void networkWrite(int socket, const void *buffer, size_t size);

typedef struct _vector_struct Vector;

typedef struct _server_info_struct {
    int serverSocket;
    sem_t semaphore;
    Vector *mappers;
    Vector *reducers;
    pthread_mutex_t mutex;
} ServerInfo;

typedef struct _client_struct {
    int clientSocket;
    in_addr_t clientAddress;
    pthread_t clientThread;
} Client;

ServerInfo startServer(uint16_t portNumber);

int getReducerForKey(const char *key, Vector *reducers, size_t *reducerIndex);

void addReducer(const char *key, Vector *reducers, size_t reducerIndex);

#endif