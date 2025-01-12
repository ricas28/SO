#ifndef __SERVER_CLIENT__H__
#define __SERVER_CLIENT__H__

#include "src/common/constants.h"

#define BUFFER_SIZE 10

typedef struct {
  sem_t empty, full, active_sessions;
  pthread_mutex_t buffer_mutex;
  int write_Index;
  int read_Index;
  char buffer[BUFFER_SIZE][MAX_REGISTER_MSG];
}Server_data;

typedef struct{
  Server_data *server_data;
  char *register_FIFO;
}Host_thread;

/// Creates a new server_data object.
/// @param empty Semaphore 
/// @param full Semaphore
/// @param buffer_mutex Mutex for buffer.
/// @param buffer Request buffer.
/// @return Pointer for new object, NULL on error.
Server_data *new_server_data();

/// Destroys a server_data object.
/// @param server_data 
void destroy_server_data(Server_data *server_data);

/// Thread function for conecting a client to the server.
/// @param arg buffer for requests.
/// @return NULL
void* host_thread_fn(void* arg);

/// Thread function to take care of client requests.
/// @param arg buffer for requests.
/// @return NULL
void* managing_thread_fn(void *arg);

#endif