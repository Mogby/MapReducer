cmake_minimum_required(VERSION 3.6)
project(MapReducer)

set(CMAKE_C_STANDARD 11)

set(MAPPER_SOURCE src/mapper.c src/common.c)
add_executable(Mapper ${MAPPER_SOURCE})

set(REDUCER_SOURCE src/reducer.c src/common.c)
add_executable(Reducer ${REDUCER_SOURCE})

set(WORKER_MANAGER_SOURCE src/worker_manager.c src/common.c src/communication.c)
add_executable(WorkerManager ${WORKER_MANAGER_SOURCE})
target_link_libraries(WorkerManager -lpthread)

set(MASTER_MANAGER_SOURCE src/master_manager.c src/common.c src/communication.c)
add_executable(MasterManager ${MASTER_MANAGER_SOURCE})
target_link_libraries(MasterManager -lpthread)