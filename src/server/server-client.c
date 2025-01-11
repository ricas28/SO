#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <semaphore.h>
#include <fcntl.h>
#include <sys/select.h>

#include "src/common/constants.h"
#include "src/common/io.h"
#include "src/common/protocol.h"
#include "io.h"
#include "parser.h"
#include "constants.h"
#include "server-client.h"
#include "operations.h"

Server_data *new_server_data(){
  Server_data *new_thread = (Server_data*)malloc(sizeof(Server_data));

  /** Allocate memory. */
  if(new_thread == NULL){
    fprintf(stderr, "Failure to allocate memory for new server_thread struct.\n");
    return NULL;
  }

  /** Initialize. */
  if(sem_init(&new_thread->empty, 0, BUFFER_SIZE) == -1){
    fprintf(stderr, "Failure to initalize empty semaphore.\n");
    destroy_server_data(new_thread);
    return NULL;
  }
  if(sem_init(&new_thread->full, 0, 0) == -1){
    fprintf(stderr, "Failure to initalize full semaphore.\n");
    destroy_server_data(new_thread);
    return NULL;
  }
  if(pthread_mutex_init(&new_thread->buffer_mutex, NULL) != 0){
    fprintf(stderr, "Failure to initialize buffer mutex.\n");
    destroy_server_data(new_thread);
    return NULL;
  }

  new_thread->write_Index = 0;
  new_thread->read_Index = 0;
  return new_thread;
}

void destroy_server_data(Server_data *server_data){
  if (server_data) {
    sem_destroy(&server_data->empty);
    sem_destroy(&server_data->full);
    pthread_mutex_destroy(&server_data->buffer_mutex);
    free(server_data);
  }
}

char* consume_request(Server_data *server_data){
  char* request_message = (char*)malloc(MAX_REGISTER_MSG*sizeof(char));
  
  sem_wait(&server_data->full); 
  pthread_mutex_lock(&server_data->buffer_mutex); 
  memcpy(request_message, server_data->buffer[server_data->read_Index], MAX_REGISTER_MSG);
  server_data->read_Index = (server_data->read_Index + 1) % BUFFER_SIZE;
  pthread_mutex_unlock(&server_data->buffer_mutex); 
  sem_post(&server_data->empty); 

  return request_message;
}

void produce_request(Server_data *server_data, char *message){
  /** Wait until there's space to put message. */
  sem_wait(&server_data->empty);
  pthread_mutex_lock(&server_data->buffer_mutex);
  memcpy(server_data->buffer[server_data->write_Index], message, MAX_REGISTER_MSG);
  server_data->write_Index = (server_data->write_Index + 1) % BUFFER_SIZE;
  pthread_mutex_unlock(&server_data->buffer_mutex);
  /** Signal that there's a new mensage to read. */
  sem_post(&server_data->full);
}

void* managing_thread_fn(void *arg){
  Server_data *server_data = (Server_data*) arg;
  int req_fd, resp_fd, notif_fd, error = 0;
  char *connect_message;
  char req_pipe[MAX_PIPE_PATH_LENGTH];
  char resp_pipe[MAX_PIPE_PATH_LENGTH];
  char notif_pipe[MAX_PIPE_PATH_LENGTH];

  /** Consume a connect request. */
  connect_message = consume_request(server_data);
  strncpy(req_pipe, connect_message+1, MAX_PIPE_PATH_LENGTH);
  strncpy(resp_pipe, connect_message+MAX_PIPE_PATH_LENGTH + 1, MAX_PIPE_PATH_LENGTH);
  strncpy(notif_pipe, connect_message+MAX_PIPE_PATH_LENGTH*2 + 1, MAX_PIPE_PATH_LENGTH);
  free(connect_message);

  /** Open response pipe. */
  if((resp_fd = open(resp_pipe, O_WRONLY)) == -1){
    fprintf(stderr, "Failure to open response pipe.");
    return NULL;
  }

  /** Connect was successful. */
  if(write_all(resp_fd, "10", 2) == -1){
    fprintf(stderr, "Failure to write connect mensage.");
    close(resp_fd);
    return NULL;
  }

  /** Open request pipe. */
  if((req_fd = open(req_pipe, O_RDONLY)) == -1){
    fprintf(stderr, "Failure to open request pipe.\n");
    close(resp_fd);
    return NULL;
  }

  /** Open notification pipe. */
  if((notif_fd = open(notif_pipe, O_WRONLY)) == -1){
    fprintf(stderr, "Failure to open notification pipe.\n");
    close(resp_fd);
    close(req_fd);
    return NULL;
  }

  while(error == 0){
    char request_message[MAX_REGISTER_MSG]; 
    /** Only read if there's something to read. */
    read_all(req_fd, request_message, 1, NULL);
    switch (request_message[0]) {
      case OP_CODE_DISCONNECT:
        break;

      case OP_CODE_SUBSCRIBE:
        if(read_all(req_fd, request_message + 1, MAX_STRING_SIZE + 1, NULL) == -1){
          fprintf(stderr, "Failure to parse subsribe request.\n");
          error = 1;
        }
        if(subscribe_key(request_message + 1, notif_fd)){
          /** Key was nout found. */
          if(write_all(resp_fd, "30", 2) == -1){
            fprintf(stderr, "Failure to write subscribe (Key not found).\n");
            error = 1;
          }
        }
        else{
          if(write_all(resp_fd, "31", 2) == -1){
            fprintf(stderr, "Failure to write subscribe (success).\n");
            error = 1;
          }
        }
        break;

      case OP_CODE_UNSUBSCRIBE:
        if(read_all(req_fd, request_message + 1, MAX_STRING_SIZE + 1, NULL) == -1){
          fprintf(stderr, "Failure to parse subsribe request.\n");
          error = 1;
        }
        if(unsubscribe_key(request_message + 1, notif_fd)){
          if(write_all(resp_fd, "41", 2) == -1){
            fprintf(stderr, "Failure to write unsubscribe (key not found)\n");
            error = 1;
          }
        }
        else{
          if(write_all(resp_fd, "40", 2) == -1){
            fprintf(stderr, "Failure to unsubscribe (success).\n");
            error = 1;
          }
        }
        break;

      default:
        printf("Strange OP.\n");
        break;
    }
  }

  close(resp_fd);
  close(req_fd);
  return NULL;
}

void* host_thread_fn(void* arg){
  Host_thread *host_thread = (Host_thread*) arg;
  int fifo_fd;

  /** Open register FIFO for reading */
  fifo_fd = open(host_thread->register_FIFO, O_RDONLY); 
  if (fifo_fd == -1) {
    fprintf(stderr, "Failure opening FIFO.\n");
    return NULL;
  }
  
  while (1) {
    char *buffer = (char*) calloc(MAX_REGISTER_MSG, sizeof(char));
    ssize_t ret;

    if((ret =  read_all(fifo_fd, buffer, MAX_REGISTER_MSG, NULL)) == -1){
      fprintf(stderr, "Failure reading from register FIFO.\n");
      free(buffer);
      break;
    }
    /** Nothing useful was read. */
    if (ret <= 1 && (buffer[0] == '\n')) { 
      free(buffer);
      continue;
    }
    /** EOF. */
    if (ret == 0){
      fprintf(stderr, "Pipe closed.\n");
      free(buffer);
      break;
    }

    /** Put connect request on request buffer. */
    produce_request(host_thread->server_data, buffer);
    free(buffer);
  }
  
  close(fifo_fd);
  return NULL;
}