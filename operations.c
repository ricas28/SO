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

int kvs_backup(char file_name[], int* backups_left, int max_backups){
  pid_t pid = fork();
  int status;
  if (pid < 0){
    return 1;
  }
  else if(pid == 0){
    size_t length = strlen(file_name);
    char new_file[length-2]; 
    strncpy(new_file, file_name, length-4);
    new_file[length-3] = '\0';
    size_t backup_number = (size_t) (max_backups - *backups_left + 1);
    size_t numsize = 0;
    for (size_t backup_number_copy = backup_number; backup_number_copy > 0; backup_number_copy/=10){
      numsize++;
    }
    char buffer[length - 4 + numsize + 5*sizeof(char)];
    snprintf(buffer, sizeof(buffer), "(-%zd.bck)", backup_number);
    char new_file_name[strlen(new_file) + strlen(buffer)]; 
    strcat(new_file_name, new_file); //fix this
    strcat(new_file_name, buffer); //fix this
    int fd = open(new_file_name, O_CREAT | O_TRUNC | O_WRONLY, S_IRUSR, S_IWUSR);
    if (fd == -1)
      fprintf(stderr, "Failed backup file opening.\n");
    kvs_show(fd);
    backups_left--;
    return 0;
  }
  else{
    if(*backups_left == 0){
      wait(&status);
      backups_left--;
      kvs_backup(file_name, backups_left, max_backups);
      return 1;
    }
    else{
      return 0;
    }
  }    
  return 0;
}

void kvs_wait(unsigned int delay_ms) {
  struct timespec delay = delay_to_timespec(delay_ms);
  nanosleep(&delay, NULL);
}