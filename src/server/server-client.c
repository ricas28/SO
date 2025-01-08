#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <fcntl.h>

#include "src/common/constants.h"
#include "src/common/io.h"
#include "parser.h"
#include "constants.h"

#define OP_DISCONNECT '2'
#define OP_SUBSCRIBE '3'
#define OP_UNSUBSCRIBE '4'
#define UNKOWN_OP '5'

typedef struct Managing_thread{
  char req_pipe[MAX_PIPE_PATH_LENGTH];
  char rep_pipe[MAX_PIPE_PATH_LENGTH];
  char notif_pipe[MAX_PIPE_PATH_LENGTH];
  int connect_error;
}Managing_thread;

Managing_thread *new_managing_thread(const char *buffer, int connect_error){
    Managing_thread *new_thread;
    if((new_thread = (Managing_thread *)malloc(sizeof(Managing_thread))) == NULL){
        fprintf(stderr, "Failure to alocate memory for managing thread.\n");
        return NULL;
    }
    /** Separate the different pipes. */
    strncpy(new_thread->req_pipe, buffer+1, MAX_PIPE_PATH_LENGTH);
    strncpy(new_thread->rep_pipe, buffer+MAX_PIPE_PATH_LENGTH + 1, MAX_PIPE_PATH_LENGTH);
    strncpy(new_thread->notif_pipe, buffer+MAX_PIPE_PATH_LENGTH*2 + 1, MAX_PIPE_PATH_LENGTH);
    new_thread->connect_error = connect_error;
    return new_thread;
}

void* managing_thread_fn(void *arg){
  Managing_thread *managing_thread = (Managing_thread *)arg;
  int resp_fd, req_fd;
  
  /** Open response pipe. */
  if ((resp_fd = open(managing_thread->rep_pipe, O_WRONLY)) == -1){
    fprintf(stderr, "Failure opening response FIFO.\n");
    /** If FIFO wasn't opened just quit. */
    free(managing_thread);
    managing_thread->connect_error = 1;
    return NULL;
  }
  /** Write mensage for connect. */
  if(managing_thread->connect_error == 0){
    if(write_all(resp_fd, "10", 2) == -1){
      fprintf(stderr, "Failure writing success mensage for connect.\n");
      close(resp_fd);
      free(managing_thread);
      managing_thread->connect_error = 1;
      return NULL;
    }
  }
  else{
    if(write_all(resp_fd, "11", 2) == -1){
      fprintf(stderr, "Failure writing error mensage for connect.\n");
      close(resp_fd);
      free(managing_thread);
      managing_thread->connect_error = 1;
      return NULL;
    }
  }
    
  /** Open request pipe. */
  if((req_fd = open(managing_thread->req_pipe, O_RDONLY)) == -1){
    fprintf(stderr, "Failure to open request FIFO.\n");
    close(resp_fd);
    free(managing_thread);
    managing_thread->connect_error = 1;
    return NULL;
  }

  /** Read all request from clients. */
  while (1) {
    /** Read the opcode. */
    /** TODO: Remove temporary mensages and read whole request. */
    char op_code[1];
    switch (read(req_fd, op_code, 1)) {
      case OP_DISCONNECT:
        printf("disconnect.\n");
        break;

      case OP_SUBSCRIBE:
        printf("subscribe.\n");
        break;

      case OP_UNSUBSCRIBE:
        printf("unsubcribe.\n");
        break;
     
      default:
       printf("Strange OP.\n");
    }
  }

  close(resp_fd);
  close(req_fd);
  free(managing_thread);
  return NULL;
}

void* host_thread_fn(void* arg){
  const char* fifo_name = (const char*) arg;
  int fifo_fd, error = 0, active_sessions = 0;
  pthread_t client_threads[MAX_SESSION_COUNT];

  /** Open register FIFO for reading */
  fifo_fd = open(fifo_name, O_RDONLY); 
  if (fifo_fd == -1) {
    fprintf(stderr, "Failure opening FIFO.\n");
    error = 1;
  }
  
  while (error == 0) {
    char *buffer = (char*) calloc(MAX_REGISTER_MSG, sizeof(char));
    int intr;
    ssize_t ret;

    if((ret =  read_all(fifo_fd, buffer, MAX_REGISTER_MSG, &intr)) == -1){
      fprintf(stderr, "Failure reading from register FIFO.\n");
      error = 1;
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

    /** New managing thread. */
    Managing_thread *new_thread;
    if((new_thread = new_managing_thread(buffer, error)) == NULL){
        error = 1;
    }
    if(pthread_create(&client_threads[active_sessions++], NULL, managing_thread_fn, (void *)new_thread) != 0){
      fprintf(stderr, "Failure creating managing thread.\n");
      error = 1;
    }

    free(buffer);
  }
  
  for(int i = 0; i < MAX_SESSION_COUNT; i++){
    if(pthread_join(client_threads[i], NULL) != 0)
      fprintf(stderr, "Failure joining %d managing thread\n", i);
  }
  close(fifo_fd);
  return NULL;
}