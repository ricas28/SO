#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>

#include "kvs.h"
#include "constants.h"

static struct HashTable* kvs_table = NULL;


/// Calculates a timespec from a delay in milliseconds.
/// @param delay_ms Delay in milliseconds.
/// @return Timespec with the given delay.
static struct timespec delay_to_timespec(unsigned int delay_ms) {
  return (struct timespec){delay_ms / 1000, (delay_ms % 1000) * 1000000};
}

int write_buffer(int fd, char *buffer, size_t buffer_size){
  /** Check if buffer isn't NULL.  */
  if(!buffer)
    return -1;
  size_t len = buffer_size;
  size_t done = 0;
  /** Sometimes write system call, won't write everything. */
  while (len > done) {
    ssize_t bytes_written = write(fd, buffer + done, len - done);
    /**  Error while writing. */
    if (bytes_written < 0) {
      fprintf(stderr, "Failed to Write buffer\n");
      return -1;
    }
    done += (size_t)bytes_written;
  }
  /** Write operation was successful. */
  return 0;
}

int kvs_init() {
  if (kvs_table != NULL) {
    fprintf(stderr, "KVS state has already been initialized\n");
    return 1;
  }

  kvs_table = create_hash_table();
  return kvs_table == NULL;
}

int kvs_terminate() {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  free_table(kvs_table);
  return 0;
}

int kvs_write(size_t num_pairs, char keys[][MAX_STRING_SIZE], char values[][MAX_STRING_SIZE]) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  for (size_t i = 0; i < num_pairs; i++) {
    if (write_pair(kvs_table, keys[i], values[i]) != 0) {
      fprintf(stderr, "Failed to write keypair (%s,%s)\n", keys[i], values[i]);
    }
  }

  return 0;
}

int kvs_read(size_t num_pairs, char keys[][MAX_STRING_SIZE], int fd) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  write(fd, "[", 1*sizeof(char));
  for (size_t i = 0; i < num_pairs; i++) {
    // strlen("(,KVSERROR)") = 11
    char buffer[MAX_STRING_SIZE + 11*sizeof(char) + 1];
    char* result = read_pair(kvs_table, keys[i]);

    if (result == NULL) {
      snprintf(buffer, sizeof(buffer), "(%s,KVSERROR)", keys[i]);
      write_buffer(fd, buffer, strlen(keys[i]) + 11*sizeof(char));
    } else {
      snprintf(buffer, sizeof(buffer), "(%s,%s)", keys[i], result);
      /** strlen("(,)") = 3. */
      write_buffer(fd, buffer,  strlen(keys[i]) + strlen(result) + 3*sizeof(char));
    }
    free(result);
  }
  write(fd, "]\n", 2*sizeof(char));
  return 0;
}

int kvs_delete(size_t num_pairs, char keys[][MAX_STRING_SIZE], int fd) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }
  int aux = 0;

  for (size_t i = 0; i < num_pairs; i++) {
    if (delete_pair(kvs_table, keys[i]) != 0) {
      if (!aux) {
        write(fd, "[", 1*sizeof(char));
        aux = 1;
      }
      /** strlen("(,KVSMISSING)") = 13.*/
      char buffer[strlen(keys[i]) + 13*sizeof(char) + 1];
      snprintf(buffer, sizeof(buffer), "(%s,KVSMISSING)", keys[i]);
      write_buffer(fd, buffer, sizeof(buffer) - 1);
    }
  }
  if (aux) {
    write(fd, "]\n", 2*sizeof(char));
  }

  return 0;
}

void kvs_show(int fd) {
  for (int i = 0; i < TABLE_SIZE; i++) {
    KeyNode *keyNode = kvs_table->table[i];
    while (keyNode != NULL) {
      char* key = keyNode->key;
      char* value = keyNode->value;
      /** strlen("(, )\n") = 5. */
      char buffer[2*MAX_STRING_SIZE + 5 *sizeof(char) + 1];
      if(snprintf(buffer, sizeof(buffer), "(%s, %s)\n", key, value) == -1){
        fprintf(stderr, "Error alocating memory on SHOW command.\n");
        continue;
      }
      write_buffer(fd, buffer,  strlen(key) + strlen(value) + 5 *sizeof(char));
      keyNode = keyNode->next; // Move to the next node
    }
  }
}

int create_backup_file(char file_name[], size_t backup_number){
  size_t length = strlen(file_name);
  /** We only want to keep the actual file name, instead of the ".job". */
  char job_file_name[length - 3]; 
  /** Copy only the actual file name. */
  strncpy(job_file_name, file_name, length-4);
  job_file_name[length-4] = '\0';

  /** Calculate length of backup_number. */
  size_t numsize = 0;
  for (size_t backup_number_copy = backup_number; backup_number_copy > 0; backup_number_copy/=10)
    numsize++;

  /** Create a buffer for sufix of file name.. */
  /** strlen("-.bck") = 5 */
  char buffer[numsize + 5*sizeof(char) + 1];
  snprintf(buffer, sizeof(buffer), "-%zd.bck", backup_number);

  /** Create a "string" that can hold the whole backup file's name.*/
  char new_file_name[length - 4 + numsize + 5*sizeof(char) + 1]; 
  strncpy(new_file_name, job_file_name, length - 4); 
  new_file_name[length - 4] = '\0';
  strncat(new_file_name, buffer, numsize + 5*sizeof(char)); 

  /** Open backup file. */
  int fd = open(new_file_name, O_CREAT | O_TRUNC | O_WRONLY, S_IRUSR | S_IWUSR);
  return fd;
}

int kvs_backup(char file_name[], size_t* backups_done, size_t *backups_left){
  pid_t pid;
  int status;

  /** Wait until there's backups to do. */
  if(*backups_left == 0){
    /** Wait until a child process finishes. */
    wait(&status);
    /** There's a new backup that can be done. */
    (*backups_left)++;  
  }

  /** Create a new process. */
  pid = fork();
  /** Problema creating a fork. */
  if (pid < 0){
    fprintf(stderr, "Failure creating new process for backup\n");
    return 1;
  }
  /** Child. */
  else if(pid == 0){
    int fd = create_backup_file(file_name, ++(*backups_done));
    /** Problem opening the backup file. */
    if(fd < 0){
      fprintf(stderr, "Failure creating backup file\n");
      return 1;
    }
    kvs_show(fd);
    exit(EXIT_SUCCESS);
  }
  /** Parent. */
  else{
    (*backups_left)--;
    (*backups_done)++;
    return 0;
  }    
}

void kvs_wait(unsigned int delay_ms) {
  struct timespec delay = delay_to_timespec(delay_ms);
  nanosleep(&delay, NULL);
}