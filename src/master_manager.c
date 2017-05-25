#include "common.h"
#include "communication.h"

#include <netdb.h>
#include <unistd.h>

#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#include <pthread.h>

typedef struct _mapper_struct {
    ServerInfo *server;
    Client *client;
} Mapper;

void* handleMapper(void* mapperrPointer) {
    Mapper *mapper = (Mapper*)mapperrPointer;

    for (;;) {
        u_char command;
        networkRead(mapper->client->clientSocket, &command, sizeof(command));
        if (command == COMMAND_STOP) {
            break;
        }
    }

    printf("Mapper #%d terminated\n", mapper->client->clientSocket);
    sem_post(&mapper->server->semaphore);
}

void* startListening(void* serverInfoPointer) {
    ServerInfo *server = serverInfoPointer;

    listen(server->serverSocket, 16);

    int clientSocket;
    struct sockaddr_in clientAddress;
    size_t clientAddressLength = sizeof(clientAddress);

    for(;;) {
        clientSocket = accept(server->serverSocket,
                              (struct sockaddr*)&clientAddress,
                              (socklen_t *)&clientAddressLength);

        pthread_mutex_lock(&server->mutex);

        if (clientSocket < 0) {
            perror("ERROR accepting client");
            continue;
        }

        Client *newClient = malloc(sizeof(Client));

        newClient->clientSocket = clientSocket;
        newClient->clientAddress = clientAddress.sin_addr.s_addr;

        printf("Client #%d connected\n", newClient->clientSocket);

        u_char registerCommand;
        networkRead(newClient->clientSocket, &registerCommand, sizeof(registerCommand));

        if (registerCommand == COMMAND_REGISTER_MAPPER) {
            printf("Client #%d is a mapper\n", newClient->clientSocket);
            pushBack(server->mappers, (void*)newClient);
        } else if (registerCommand == COMMAND_REGISTER_REDUCER) {
            printf("Client #%d is a reducer\n", newClient->clientSocket);
            pushBack(server->reducers, (void*)newClient);
        } else {
            printf("Client #%d failed\n", newClient->clientSocket);
        }

        pthread_mutex_unlock(&server->mutex);
    }

    return NULL;
}

int main() {
    ServerInfo server = startServer(MASTER_PORT_NUMBER);

    pthread_t listener;
    pthread_create(&listener, NULL, &startListening, (void*)&server);

    puts("Started listening");
    puts("Input anything to start MapReduce");

    char inputBuffer[256];
    scanf("%s", inputBuffer);

    pthread_mutex_lock(&server.mutex);
    pthread_cancel(listener);
    pthread_mutex_unlock(&server.mutex);

    for (size_t mapperIndex = 0; mapperIndex < server.mappers->size; ++mapperIndex) {
        Client *mapper = (Client*)server.mappers->storage[mapperIndex];

        networkWrite(mapper->clientSocket, &COMMAND_REGISTER_REDUCER, sizeof(COMMAND_REGISTER_REDUCER));
        networkWrite(mapper->clientSocket, &server.reducers->size, sizeof(server.reducers->size));
        for (size_t reducerIndex = 0; reducerIndex < server.reducers->size; ++reducerIndex) {
            Client *reducer = (Client*)server.reducers->storage[reducerIndex];

            networkWrite(mapper->clientSocket, &reducer->clientAddress, sizeof(reducer->clientAddress));

            printf("Notifying mappper #%d of reducer #%d\n", mapper->clientSocket, reducer->clientSocket);
        }

        Mapper *mapperArgs = malloc(sizeof(Mapper));
        mapperArgs->client = mapper;
        mapperArgs->server = &server;

        pthread_create(&mapper->clientThread, NULL, &handleMapper, (void*)mapperArgs);
    }

    for (size_t index = 0; index < server.mappers->size; ++index) {
        sem_wait(&server.semaphore);
    }

    for (size_t index = 0; index < server.reducers->size; ++index) {
        networkWrite(((Client*)server.reducers->storage[index])->clientSocket, &COMMAND_STOP, sizeof(COMMAND_STOP));
    }

    return 0;
}