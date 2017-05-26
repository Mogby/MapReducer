#include "common.h"
#include "communication.h"

#include <unistd.h>
#include <netdb.h>
#include <pthread.h>

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <ctype.h>
#include <wchar.h>
#include <wait.h>
#include <fcntl.h>

static int outputFile;
static pthread_mutex_t outputMutex;

static Vector *keyReducers;

typedef struct _connection_struct {
    int connectionSocket;
    struct sockaddr_in hostAddress;
} Connection;

Connection connectToHost(const char *hostname) {
    Connection result;

    result.connectionSocket = socket(AF_INET, SOCK_STREAM, 0);

    if (result.connectionSocket < 0) {
        perror("ERROR opening socket");
        exit(1);
    }

    puts("Socket opened");

    struct hostent *server = gethostbyname(hostname);

    if (!server) {
        perror("ERROR looking host up");
        exit(1);
    }

    puts("Host found");

    bzero(&result.hostAddress, sizeof(result.hostAddress));
    result.hostAddress.sin_family = AF_INET;
    bcopy(&result.hostAddress.sin_addr.s_addr, &server->h_addr, server->h_length);
    result.hostAddress.sin_port = htons(MASTER_PORT_NUMBER);

    if (connect(result.connectionSocket, (struct sockaddr*)&result.hostAddress, sizeof(result.hostAddress)) < 0) {
        perror("ERROR connecting to the host");
        exit(1);
    }

    puts("Connected to the host");

    return result;
}

typedef struct _reducer_struct {
    int socket;
    in_addr_t address;
} Reducer;

size_t askForReducer(const char *key, Connection connection) {
    size_t result;

    if (getReducerForKey(key, keyReducers, &result) < 0) {
        printf("Asking reducer for key %s\n", key);
        networkWrite(connection.connectionSocket, &COMMAND_GET_REDUCER, sizeof(COMMAND_GET_REDUCER));

        size_t keySize = strlen(key);
        networkWrite(connection.connectionSocket, &keySize, sizeof(key));
        write(connection.connectionSocket, key, keySize);

        networkRead(connection.connectionSocket, &result, sizeof(result));

        addReducer(key, keyReducers, result);
    }

    return result;
}

void runMapper(Connection connection) {
    keyReducers = createVector();

    int mapperPipe[2];

    pipe(mapperPipe);

    fflush(stdout);
    if (!fork()) {
        close(mapperPipe[0]);
        close(STDOUT_FILENO);
        close(connection.connectionSocket);
        dup(mapperPipe[1]);
        execl("Mapper", "Mapper", NULL);
    }

    close(mapperPipe[1]);
    close(STDIN_FILENO);
    dup(mapperPipe[0]);
    wait(NULL);

    write(connection.connectionSocket, &COMMAND_REGISTER_MAPPER, sizeof(COMMAND_REGISTER_MAPPER));

    u_char command;
    networkRead(connection.connectionSocket, &command, sizeof(command));

    if (command != COMMAND_REGISTER_REDUCER) {
        puts("Server won't send list of reducers");
        exit(1);
    }

    size_t reducersCount;
    networkRead(connection.connectionSocket, &reducersCount, sizeof(reducersCount));

    printf("Total %lu reducers\n", reducersCount);

    Reducer *newReducer;
    Vector *reducers = createVector();
    for (size_t index = 0; index < reducersCount; ++index) {
        newReducer = malloc(sizeof(Reducer));

        networkRead(connection.connectionSocket, &newReducer->address, sizeof(in_addr_t));

        printf("Reducer with address %u.%u.%u.%u\n", newReducer->address & 255, (newReducer->address >> 8) & 255,
               (newReducer->address >> 16) & 255, (newReducer->address >> 24) & 255);

        newReducer->socket = socket(AF_INET, SOCK_STREAM, 0);
        if (newReducer->socket < 0) {
            perror("ERROR opening socket");
            free(newReducer);
            continue;
        }
        puts("Opened socket");

        struct sockaddr_in reducerAddress;
        bzero(&reducerAddress, sizeof(reducerAddress));
        reducerAddress.sin_family = AF_INET;
        reducerAddress.sin_addr.s_addr = newReducer->address;
        reducerAddress.sin_port = htons(REDUCER_PORT_NUMBER);

        if (connect(newReducer->socket, (struct sockaddr*)&reducerAddress, sizeof(reducerAddress)) < 0) {
            perror("ERROR connecting to reducer");
            close(newReducer->socket);
            free(newReducer);
            continue;
        }
        networkWrite(newReducer->socket, &COMMAND_REGISTER_MAPPER, sizeof(COMMAND_REGISTER_MAPPER));

        puts("Connected to reducer");

        pushBack(reducers, (void*)newReducer);
    }

    char key[1024], value[1024];
    size_t reducerIndex;
    while (scanf("%s : %s", key, value) == 2) {
        reducerIndex = askForReducer(key, connection);

        networkWrite(((Reducer*)reducers->storage[reducerIndex])->socket,
                     &COMMAND_HANDLE_KEYVALUE, sizeof(COMMAND_HANDLE_KEYVALUE));

        size_t keySize = sizeof(key[0]) * strlen(key);
        size_t valueSize = sizeof(value[0]) * strlen(value);

        networkWrite(((Reducer*)reducers->storage[reducerIndex])->socket, &keySize, sizeof(keySize));
        write(((Reducer*)reducers->storage[reducerIndex])->socket, key, keySize);

        networkWrite(((Reducer*)reducers->storage[reducerIndex])->socket, &valueSize, sizeof(valueSize));
        write(((Reducer*)reducers->storage[reducerIndex])->socket, value, valueSize);
    }

    networkWrite(connection.connectionSocket, &COMMAND_STOP, sizeof(COMMAND_STOP));
    for (size_t index = 0; index < reducers->size; ++index) {
        networkWrite(((Reducer*)reducers->storage[index])->socket, &COMMAND_STOP, sizeof(COMMAND_STOP));
    }
}

void outputPair(const char *key, size_t keySize, const char *value, size_t valueSize) {
    pthread_mutex_lock(&outputMutex);

    write(outputFile, key, keySize);
    write(outputFile, " : ", 3);
    write(outputFile, value, valueSize);
    write(outputFile, "\n", 1);

    pthread_mutex_unlock(&outputMutex);
}

void* handleClient(void *clientPointer) {
    Client *client = (Client*)clientPointer;

    u_char command;
    char key[1024], value[1024];

    for (;;) {
        networkRead(client->clientSocket, &command, sizeof(command));

        if (command == COMMAND_STOP) {
            printf("Stopped handling client #%d\n", client->clientSocket);
            break;
        } else if (command == COMMAND_HANDLE_KEYVALUE) {
            size_t keySize;
            size_t valueSize;

            networkRead(client->clientSocket, &keySize, sizeof(keySize));
            read(client->clientSocket, key, keySize);

            networkRead(client->clientSocket, &valueSize, sizeof(valueSize));
            read(client->clientSocket, value, valueSize);

            outputPair(key, keySize, value, valueSize);
        } else {
            printf("Client #%d\n failed", client->clientSocket);
            break;
        }
    }

    return NULL;
}

void* startListening(void *serverPointer) {
    ServerInfo *server = (ServerInfo*)serverPointer;

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
            pushBack(server->mappers, (void*)newClient);
            pthread_create(&newClient->clientThread, NULL, &handleClient, (void*)newClient);
        } else {
            printf("Client #%d failed\n", newClient->clientSocket);
        }

        pthread_mutex_unlock(&server->mutex);
    }

    return NULL;
}

void runReducer(Connection connection) {
    outputFile = open("manager.out", O_WRONLY | O_CREAT, S_IWUSR | S_IRUSR);
    pthread_mutex_init(&outputMutex, NULL);

    networkWrite(connection.connectionSocket, &COMMAND_REGISTER_REDUCER, sizeof(COMMAND_REGISTER_REDUCER));

    ServerInfo server = startServer(REDUCER_PORT_NUMBER);
    pthread_t listener;
    pthread_create(&listener, NULL, startListening, (void*)&server);

    for (;;) {
        u_char command;
        networkRead(connection.connectionSocket, &command, sizeof(command));
        if (command == COMMAND_STOP) {
            break;
        }
    }

    pthread_mutex_lock(&server.mutex);
    pthread_cancel(listener);
    pthread_mutex_unlock(&server.mutex);

    for (size_t index = 0; index < server.mappers->size; ++index) {
        pthread_join(((Client*)server.mappers->storage[index])->clientThread, NULL);
    }

    close(connection.connectionSocket);
    close(outputFile);

    if (!fork()) {
        int inputFile = open("manager.out", O_RDONLY);
        dup2(inputFile, STDIN_FILENO);
        close(inputFile);
        execl("Reducer", "Reducer", NULL);
    }

    wait(NULL);
}


int main() {
    char inputBuffer[256] = "";
    char isMapper;

    while (tolower(inputBuffer[0]) != 'y' && tolower(inputBuffer[0]) != 'n') {
        puts("Starting mapper? (y/n)");
        scanf("%s", inputBuffer);
        isMapper = (tolower(inputBuffer[0]) == 'y');
    }

    if (isMapper) {
        puts("Starting mapper");
    } else {
        puts("Starting reducer");
    }

    puts("Input hostname:");
    scanf("%s", inputBuffer);

    Connection connection = connectToHost(inputBuffer);

    if (isMapper) {
        runMapper(connection);
    } else {
        runReducer(connection);
    }

    return 0;
}