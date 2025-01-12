#include <limits.h>
#include <fcntl.h>
#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <pthread.h>
#include <semaphore.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <signal.h>

#include "constants.h"
#include "parser.h"
#include "operations.h"
#include "file_processor.h"
#include "server-client.h"
#include "src/common/constants.h"
#include "src/common/io.h"

/** Ignore SIGPIPE (client disconnects and server can't crash). */
void setup_SIGPIPE_ignore(){
  signal(SIGPIPE, SIG_IGN);
}

int main(int argc, char** argv) {
  pthread_mutex_t backup_mutex;
  DIR* pDir;
  pthread_t host_thread, managing_threads[MAX_SESSION_COUNT];
  Server_data *server_data;
  Host_thread host_thread_data;

  if (kvs_init()) {
    fprintf(stderr, "Failed  to initialize KVS\n");
    return 1;
  }

  if(argc < 5){
    fprintf(stderr, "Usage: %s <directory_path> <max_backups> <max_threads> <pipe_path>\n", argv[0]);
    return 1;
  }
  const size_t MAX_BACKUPS = (size_t)strtoul(argv[2], NULL, 10);
  const size_t MAX_THREADS = (size_t)strtoul(argv[3], NULL, 10);
  
  setup_SIGPIPE_ignore();

  /** Open directory. */
  if((pDir = opendir(argv[1])) == NULL){
    fprintf(stderr, "Failed to open directory.\n");
    kvs_terminate();
    return 1;
  }

  /** Initialize mutex for backup. */
  if(pthread_mutex_init(&backup_mutex, NULL) != 0){
    fprintf(stderr, "Failed to initialize backup mutex.\n");
    kvs_terminate();
    closedir(pDir);
    return 1;
  }

  /** Erase previous FIFO. */
  unlink(argv[4]);
  /** Open register FIFO. */
  if (mkfifo(argv[4], 0666) != 0) {
    fprintf(stderr, "Failure creating register FIFO.\n");
    kvs_terminate();
    closedir(pDir);
    return 1;
  }
  
  /** Create struct for server-client threads. */
  if((server_data = new_server_data()) == NULL){
    kvs_terminate();
    closedir(pDir);
    return 1;
  }
  /** Initalize host_thread_data. */
  host_thread_data.server_data = server_data;
  host_thread_data.register_FIFO = argv[4];
  /** Create host thread. */
  if (pthread_create(&host_thread, NULL, host_thread_fn, (void*)&host_thread_data) == -1){
    fprintf(stderr, "Error creating host thread.\n");
    kvs_terminate();
    closedir(pDir);
    destroy_server_data(server_data);
    return 1;
  }
  /** Create managing threads. */
  for(int i = 0; i < MAX_SESSION_COUNT; i++){
    if(pthread_create(&managing_threads[i], NULL, managing_thread_fn, (void*)server_data) == -1){
      fprintf(stderr, "Failure to create managing thread %d.\n", i + 1);
      kvs_terminate();
      closedir(pDir);
      destroy_server_data(server_data);
      return 1;
    }
  }

  /** Start processing .job files. */
  if(dispatch_job_threads(argv[1], MAX_BACKUPS, MAX_THREADS, &backup_mutex, pDir) == 1){
    kvs_terminate();
    closedir(pDir);
    return 1;
  }
  
  for(int i = 0; i < MAX_SESSION_COUNT; i++){
    if(pthread_join(managing_threads[i], NULL) != 0){
      fprintf(stderr, "Failed to join managing thread %d\n", i);
    }
  }
  
  if (pthread_join(host_thread, NULL) != 0){
    fprintf(stderr, "Failed to join host thread.\n");
    kvs_terminate();
    closedir(pDir);
    destroy_server_data(server_data);
    return 1;
  }

  /** Ending program. */
  kvs_terminate();
  closedir(pDir);
  destroy_server_data(server_data);
  return 0;
}