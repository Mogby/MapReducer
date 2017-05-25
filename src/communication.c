#include "communication.h"
#include "common.h"

#include <unistd.h>
#include <netinet/in.h>

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

