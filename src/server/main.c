#include <limits.h>
#include <fcntl.h>
#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <pthread.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "constants.h"
#include "parser.h"
#include "operations.h"
#include "file_processor.h"
#include "src/common/constants.h"
#include "src/common/io.h"

typedef struct Managing_thread{
  char req_pipe[MAX_PIPE_PATH_LENGTH];
  char rep_pipe[MAX_PIPE_PATH_LENGTH];
  char notif_pipe[MAX_PIPE_PATH_LENGTH];
}Managing_thread;

 
void* managing_thread_fn(void *arg){
  Managing_thread *managing_thread = (Managing_thread *)arg;


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
    if((new_thread = (Managing_thread *)malloc(sizeof(Managing_thread))) == NULL){
      fprintf(stderr, "Failure to alocate memory for managing thread.\n");
      error = 1;
    }
    if(error == 0){
      /** Separate the different pipes. */
      strncpy(new_thread->req_pipe, buffer+1, MAX_PIPE_PATH_LENGTH);
      strncpy(new_thread->rep_pipe, buffer+MAX_PIPE_PATH_LENGTH + 1, MAX_PIPE_PATH_LENGTH);
      strncpy(new_thread->notif_pipe, buffer+MAX_PIPE_PATH_LENGTH*2 + 1, MAX_PIPE_PATH_LENGTH);
    }

    if(pthread_create(&client_threads[active_sessions++], NULL, managing_thread_fn, (void *)new_thread) != 0){
      fprintf(stderr, "Failure creating managing thread.\n");
      error = 1;
    }

    int rep_pipe;
    if ((rep_pipe = open(new_thread->rep_pipe, O_WRONLY)) == -1){
      fprintf(stderr, "Failure opening response FIFO.\n");
      free(buffer);
      /** If FIFO wasn't opened just quit. */
      break;
    }
    /** If this code is reached, there were no erros during connect. */
    if(error == 0){
      if(write_all(rep_pipe, "10", 2) == -1)
        fprintf(stderr, "Failure writing success mensage for connect.\n");
    }
    else{
      if(write_all(rep_pipe, "11", 2) == -1)
        fprintf(stderr, "Failure writing error mensage for connect.\n");
    }
    
    close(rep_pipe);  
    free(buffer);
  }
  
  for(int i = 0; i < MAX_SESSION_COUNT; i++){
    if(pthread_join(client_threads[i], NULL) != 0)
      fprintf(stderr, "Failure joining %d managing thread\n", i);
  }
  close(fifo_fd);
  return NULL;
}

int main(int argc, char** argv) {
  pthread_mutex_t backup_mutex;
  DIR* pDir;
  int error;
  pthread_t host_thread;

  if (kvs_init()) {
    fprintf(stderr, "Failed to initialize KVS\n");
    return -1;
  }

  if(argc < 5){
    fprintf(stderr, "Usage: %s <directory_path> <max_backups> <max_threads> <pipe_path>\n", argv[0]);
    return -1;
  }
  const size_t MAX_BACKUPS = (size_t)strtoul(argv[2], NULL, 10);
  const size_t MAX_THREADS = (size_t)strtoul(argv[3], NULL, 10);
  
  if((pDir = opendir(argv[1])) == NULL){
    fprintf(stderr, "Failed to open directory.\n");
    error = 1;
  }

  /** Initialize mutex for backup. */
  if(pthread_mutex_init(&backup_mutex, NULL) != 0){
    fprintf(stderr, "Failed to initialize backup mutex.\n");
    error = 1;
  }

  /** Erase previous FIFO. */
  unlink(argv[4]);
  /** Open register FIFO. */
  if (mkfifo(argv[4], 0666) != 0) {
    fprintf(stderr, "Failure creating register FIFO.\n");
    error = 1;
  }

  if (pthread_create(&host_thread, NULL, host_thread_fn, argv[4]) == 1){
    fprintf(stderr, "Error creating host thread.\n");
    error = 1;
  }

  if (error != 1){
    error = dispatch_threads(argv[1], MAX_BACKUPS, MAX_THREADS, &backup_mutex, pDir);
  }

  if (pthread_join(host_thread, NULL) != 0){
    fprintf(stderr, "Failed to join host thread.\n");
    error = 1;
  }

  /** Ending program. */
  kvs_terminate();
  closedir(pDir);
  /** Something went wrong during the program. */
  if(error == 1) return -1;
  return 0;
}