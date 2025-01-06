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

#include "constants.h"
#include "parser.h"
#include "operations.h"
#include "File.h"

typedef struct Thread_data{
  pthread_mutex_t *backup_mutex;
  size_t *backups_left;
  File *file;
}Thread_data;

/// Processes every command on a file.
/// @param arg pointer to arguments needed for making in and out file.
/// @return NULL
void *process_file(void *arg){
  Thread_data *thread_data = (Thread_data *)arg;
  char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
  char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
  unsigned int delay;
  size_t num_pairs;
  size_t backups_done = 0;
  int read_fd, write_fd;

  /** Build relative path of file. */
  char file_directory[get_path_size(thread_data->file)];
  if(snprintf(file_directory, sizeof(file_directory), "%s/%s", 
                                get_file_directory(thread_data->file), 
                                get_file_name(thread_data->file)) == -1){
    fprintf(stderr, "Failure to create file path.\n");
    return NULL;
  }
                                              
  /** Open input file. */
  if ((read_fd = open(file_directory, O_RDONLY)) == -1) {
    fprintf(stderr, "Error opening read file: %s\n", file_directory);
    return NULL;
  }

  /** Build relative path for the output file. */
  size_t length = strlen(file_directory);
  char write_directory[length+1]; 
  strcpy(write_directory, file_directory);
  write_directory[length-1] = 't';
  write_directory[length-2] = 'u';
  write_directory[length-3] = 'o';

  /** Open output file. */
  write_fd = open(write_directory, O_CREAT | O_TRUNC | O_WRONLY, S_IRUSR | S_IWUSR);
  if(write_fd == -1){
    fprintf(stderr, "Error opening output file\n");
    close(read_fd);
  }

  int quit = 0;
  while(!quit){
    switch (get_next(read_fd)) {
      case CMD_WRITE:
        num_pairs = parse_write(read_fd, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
        if (num_pairs == 0) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
        }

        if (kvs_write(num_pairs, keys, values)) {
          fprintf(stderr, "Failed to write pair\n");
        }
        break;

      case CMD_READ:
        num_pairs = parse_read_delete(read_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

        if (num_pairs == 0) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
        }

        if (kvs_read(num_pairs, keys, write_fd)) {
          fprintf(stderr, "Failed to read pair\n");
        }
        break;

      case CMD_DELETE:
        num_pairs = parse_read_delete(read_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

        if (num_pairs == 0) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
        }

        if (kvs_delete(num_pairs, keys, write_fd)) {
          fprintf(stderr,"Failed to delete pair\n");
        }
        break;

      case CMD_SHOW:
        kvs_show(write_fd);
        break;

      case CMD_WAIT:
        if (parse_wait(read_fd, &delay, NULL) == -1) {
          fprintf(stderr, "Failed to read pair\n");
        }

        if (delay > 0) {
          char message[] = "Waiting...\n";
          if(write(write_fd, message, sizeof(message) - 1) == -1)
            fprintf(stderr, "Failure writing WAIT message");
          kvs_wait(delay);
        }
        break;

      case CMD_BACKUP:
        if (kvs_backup(file_directory, &backups_done, thread_data->backups_left, 
                                                      thread_data->backup_mutex)) { 
          fprintf(stderr,"Failed to perform backup.\n");
        }
        break;

      case CMD_INVALID:
        fprintf(stderr, "Invalid command. See HELP for usage\n");
        break;

      case CMD_HELP:{
        char buffer[] = 
            "Available commands:\n"
            "  WRITE [(key,value)(key2,value2),...]\n"
            "  READ [key,key2,...]\n"
            "  DELETE [key,key2,...]\n"
            "  SHOW\n"
            "  WAIT <delay_ms>\n"
            "  BACKUP\n" 
            "  HELP\n";
        write_buffer(write_fd, buffer, strlen(buffer));
        break;
      }
      case CMD_EMPTY:
        break;
      case EOC:
        /** Close input and output file. */
        close(read_fd);
        close(write_fd);
        free(thread_data->file);
        free(thread_data);
        quit = 1;
    }
  }
  return NULL;
}

int dispatch_threads(char* directory_path, size_t MAX_BACKUPS, size_t MAX_THREADS, pthread_mutex_t* backup_mutex, DIR* pDir){
  int error = 0;
  struct dirent* file_dir;
  size_t backups_left = MAX_BACKUPS;
  size_t threads_index = 0;
  pthread_t threads[MAX_THREADS];
  size_t directory_size = strlen(directory_path);

  /** Keep running until there's no files to read. */
  while ((file_dir = readdir(pDir)) != NULL) {
    size_t file_name_size = strlen(file_dir->d_name);
    /** Check if file is good to open (needs to be an actual .job file). */
    if(!(file_name_size > 4 && file_dir->d_name[file_name_size-1] == 'b' && 
        file_dir->d_name[file_name_size-2] == 'o' && file_dir->d_name[file_name_size-3] == 'j' && 
        file_dir->d_name[file_name_size-4] == '.'))
          /** Go to the next file. */
          continue;
      
    Thread_data *new_thread;
    if((new_thread = (Thread_data *)malloc(sizeof(Thread_data))) == NULL){
      fprintf(stderr, "Failed to allocate memory for new thread struct.\n");
      error = 1;
    }
    new_thread->backups_left = &backups_left;
    /** Create a new File. */
    if((new_thread->file = new_file(directory_size + file_name_size + 2, directory_path, file_dir->d_name)) == NULL){ 
      free(new_thread);
      error = 1;                                                                                     
    }
    new_thread->backup_mutex = backup_mutex;

    /** Array of threads is full. */
    if(threads_index >= MAX_THREADS){
      /** Wait for a thread to finish. */
      if(pthread_join(threads[threads_index % MAX_THREADS], NULL) != 0){
        fprintf(stderr, "Failed to join thread.\n");
        error = 1;
        break;
      }
    }
    /** Create a new thread. */
    if(pthread_create(&threads[threads_index % MAX_THREADS], NULL, process_file, (void*) new_thread) != 0){
      fprintf(stderr, "Failed to create a thread.\n");
      free(new_thread);
      error = 1;
      break;
    }
    threads_index++;
  }

  /** Wait for all threads to finish. */
  for(size_t i = 0; i < MAX_THREADS && i < threads_index; i++){
    if(pthread_join(threads[i], NULL) != 0){
      fprintf(stderr, "Failed to join thread.\n");
      error = 1;
    }
  }

  /** Wait for all backups to finish. */
  while(backups_left < MAX_BACKUPS){
    wait(NULL);
    backups_left++;
  }

  /** Destroy backup mutex. */
  pthread_mutex_destroy(backup_mutex);
  return error;
}

int main(int argc, char** argv) {
  pthread_mutex_t backup_mutex;
  DIR* pDir;
  int error;

  if (kvs_init()) {
    fprintf(stderr, "Failed to initialize KVS\n");
    return -1;
  }

  if(argc < 5){
    fprintf(stderr, "Usage: %s <directory path> <max backups> <max threads> <pipe name>\n", argv[0]);
    return -1;
  }
  const size_t MAX_BACKUPS = (size_t)strtoul(argv[2], NULL, 10);
  const size_t MAX_THREADS = (size_t)strtoul(argv[3], NULL, 10);
  
  if((pDir = opendir(argv[1])) == NULL){
    fprintf(stderr, "Failed to open directory\n");
    error = 1;
  }

  /** Initialize mutex for backup. */
  if(pthread_mutex_init(&backup_mutex, NULL) != 0){
    fprintf(stderr, "Failed to initialize backup mutex\n");
    error = 1;
  }
  
  error = dispatch_threads(argv[1], MAX_BACKUPS, MAX_THREADS, &backup_mutex, pDir);

  /** Ending program. */
  kvs_terminate();
  closedir(pDir);
  /** Something went wrong during the program.. */
  if(error == 1) return -1;
  return 0;
}