#include <limits.h>
#include <fcntl.h>
#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <pthread.h>
#include <sys/wait.h>

#include "constants.h"
#include "parser.h"
#include "operations.h"
#include "File.h"

typedef struct Thread_data{
  pthread_mutex_t *backup_mutex;
  size_t *backups_left;
  File *file;
}Thread_data;

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
  snprintf(file_directory, sizeof(file_directory), "%s/%s", get_file_directory(thread_data->file), 
                                                            get_file_name(thread_data->file));
  printf("A analisar %s\n", file_directory);
  /** Open input file. */
  if ((read_fd = open(file_directory, O_RDONLY)) == -1) {
    fprintf(stderr, "Error opening read file: %s\n", file_directory);
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
          write(write_fd, message, sizeof(message) - 1);
          kvs_wait(delay);
        }
        break;

      case CMD_BACKUP:
        if (kvs_backup(file_directory, &backups_done, thread_data->backups_left, thread_data->backup_mutex)) { 
          fprintf(stderr,"Failed to perform backup.\n");
        }
        break;

      case CMD_INVALID:
        fprintf(stderr, "Invalid command. See HELP for usage\n");
        break;

      case CMD_HELP:
        char buffer[] = 
            "Available commands:\n"
            "  WRITE [(key,value)(key2,value2),...]\n"
            "  READ [key,key2,...]\n"
            "  DELETE [key,key2,...]\n"
            "  SHOW\n"
            "  WAIT <delay_ms>\n"
            "  BACKUP\n" 
            "  HELP\n"
        ;
        write_buffer(write_fd, buffer, strlen(buffer));

        break;
        
      case CMD_EMPTY:
        break;
      case EOC:
        /** Close input and output file*/
        close(read_fd);
        close(write_fd);
        free(thread_data->file);
        free(thread_data);
        quit = 1;
    }
  }
  return NULL;
}

int main(int argc, char** argv) {
  const size_t MAX_BACKUPS = (size_t)strtoul(argv[2], NULL, 10);
  size_t backups_left = MAX_BACKUPS;
  const size_t MAX_THREADS = (size_t)strtoul(argv[3], NULL, 10);
  pthread_t threads[MAX_THREADS];
  pthread_mutex_t backup_mutex;
  size_t threads_index = 0;
  DIR* pDir;

  if (kvs_init()) {
    fprintf(stderr, "Failed to initialize KVS\n");
    return -1;
  }

  if(argc < 4){
    fprintf(stderr, "Insufficient number of arguments. Use: %s <directory path> <max number of backups> <max number of threads>\n", argv[0]);
    return -1;
  }
  size_t directory_size = strlen(argv[1]);
  
  if((pDir = opendir(argv[1])) == NULL){
    fprintf(stderr, "Failed to open directory\n");
    return -1;
  }

  /** Initialize mutex for backup. */
  pthread_mutex_init(&backup_mutex, NULL);
  struct dirent* file_dir;
  /** Keep running until there's no files to read. */
  while ((file_dir = readdir(pDir)) != NULL) {
    size_t file_name_size = strlen(file_dir->d_name);
    /** Check if file is good to open (needs to be an actual .job file). */
    if(!(file_name_size > 4 && file_dir->d_name[file_name_size-1] == 'b' && 
        file_dir->d_name[file_name_size-2] == 'o' && file_dir->d_name[file_name_size-3] == 'j' && 
        file_dir->d_name[file_name_size-4] == '.'))
          continue;
      
    Thread_data *new_thread = (Thread_data *)malloc(sizeof(Thread_data));
    new_thread->backups_left = &backups_left;
    /** Create a new File. */
    new_thread->file = new_file(directory_size + file_name_size + 2, argv[1], file_dir->d_name);
    new_thread->backup_mutex = &backup_mutex;
    /** Array of threads is full. */
    if(threads_index >= MAX_THREADS){
      /** Wait for a thread to finish. */
      pthread_join(threads[threads_index % MAX_THREADS], NULL);
    }
    /** Create a new thread. */
    pthread_create(&threads[threads_index % MAX_THREADS], NULL, process_file, (void*) new_thread);
    threads_index++;
  }
  /** Wait for all backups that might not have finished. */
  int status;
  for(size_t i = 0; i < MAX_BACKUPS; i++)
    wait(&status);
  /** Wait for all threads. */
  size_t limit = threads_index > MAX_THREADS ?  MAX_THREADS : threads_index;
  for(size_t i = 0; i < limit; i++)
    pthread_join(threads[i], NULL);
  /** Destroy backup mutex. */
  pthread_mutex_destroy(&backup_mutex);
  
  /** Ending program. */
  kvs_terminate();
  closedir(pDir);
  return 0;
}
