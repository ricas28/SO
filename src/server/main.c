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
#include "server-client.h"
#include "src/common/constants.h"
#include "src/common/io.h"

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