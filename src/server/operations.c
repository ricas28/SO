#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <fcntl.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/wait.h>

#include "kvs.h"
#include "constants.h"

static struct HashTable* kvs_table = NULL;

/// Merges two sorted arrays.
/// @param keys sorted array of keys.
/// @param values values that correspond to a key.
/// @param l left limit for sorting.
/// @param m middle of the array.
/// @param r right limit for sorting.
void merge(char keys[][MAX_STRING_SIZE], char values[][MAX_STRING_SIZE], size_t l, size_t m, size_t r) {
  size_t i, j, k;
  size_t n1 = m - l + 1;
  size_t n2 = r - m;

  /** Create temp arrays */
  char L[n1][MAX_STRING_SIZE], R[n2][MAX_STRING_SIZE];
  char Lval[n1][MAX_STRING_SIZE], Rval[n2][MAX_STRING_SIZE];

  /**  Copy data to temp arrays L[] and R[] */
  for (i = 0; i < n1; i++) {
      strcpy(L[i], keys[l + i]);
      strcpy(Lval[i], values[l + i]);
  }
  for (j = 0; j < n2; j++) {
      strcpy(R[j], keys[m + 1 + j]);
      strcpy(Rval[j], values[m + 1 + j]);
  }

  /**  Merge the temp arrays back into keys[l..r] and values[l..r] */
  i = 0;
  j = 0;
  k = l;
  while (i < n1 && j < n2) {
      if (strcmp(L[i], R[j]) <= 0) {
          strcpy(keys[k], L[i]);
          strcpy(values[k], Lval[i]);
          i++;
      } else {
          strcpy(keys[k], R[j]);
          strcpy(values[k], Rval[j]);
          j++;
      }
      k++;
  }

  /**  Copy the remaining elements of L[], if there are any */
  while (i < n1) {
      strcpy(keys[k], L[i]);
      strcpy(values[k], Lval[i]);
      i++;
      k++;
  }
  /**  Copy the remaining elements of R[], if there are any */
  while (j < n2) {
      strcpy(keys[k], R[j]);
      strcpy(values[k], Rval[j]);
      j++;
      k++;
  }
}

/// Sorts keys array and keeps values conected to the corresponding key.
/// @param keys array with keys to be sorted.
/// @param values array with values that correspond to a key.
/// @param l left limit for sorting.
/// @param r right limit for sorting.
void mergeSort(char keys[][MAX_STRING_SIZE], char values[][MAX_STRING_SIZE], size_t l, size_t r){
  if (l < r) {
    size_t m = l + (r - l) / 2;

    /** Sort first and second halves */
    mergeSort(keys, values, l, m);
    mergeSort(keys, values, m + 1, r);

    /** Merge both halves. */
    merge(keys, values, l, m, r);
  }
}

/// Returns wich key has a bigger first character. 
/// @param key1 
/// @param key2 
/// @return 0 if equal, < 0 if less and > 0 if greater.
int compare_keys(const void* key1, const void *key2){
  return strncmp(key1, key2, 1);
}

/// Calculates a timespec from a delay in milliseconds.
/// @param delay_ms Delay in milliseconds.
/// @return Timespec with the given delay.
static struct timespec delay_to_timespec(unsigned int delay_ms) {
  return (struct timespec){delay_ms / 1000, (delay_ms % 1000) * 1000000};
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

/// Writes all of the content on a buffer.
/// @param fd File descriptor that will write.
/// @param buffer Buffer with content to be written.
/// @param buffer_size Size of the buffer.
/// @return 0 if successful and -1 otherwise.
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
      return -1;
    }
    done += (size_t)bytes_written;
  }
  /** Write operation was successful. */
  return 0;
}

/// Locks all of the entries on the KVS hash table, with the given keys, for writing.
/// @param num_pairs Number of keys received.
/// @param keys Array with entries that need to be blocked.
void wrlock_table_entries(size_t num_pairs, char keys[][MAX_STRING_SIZE]){
  for(size_t i = 0; i < num_pairs; i++){
    pthread_rwlock_wrlock(&kvs_table->lockTable[hash(keys[i])]);
  }
}

/// Locks all of the entries on the KVS hash table, with the given keys, for reading.
/// @param num_pairs Number of keys received.
/// @param keys Array with entries that need to be blocked.
void rdlock_table_entries(size_t num_pairs, char keys[][MAX_STRING_SIZE]){
  for(size_t i = 0; i < num_pairs; i++){
    pthread_rwlock_rdlock(&kvs_table->lockTable[hash(keys[i])]);
  }
}

/// Unlocks all of the entries on the KVS hash table, with the given keys.
/// @param num_pairs Number of keys received.
/// @param keys Array with entries that need to be unlocked.
void unlock_table_entries(size_t num_pairs, char keys[][MAX_STRING_SIZE]){
  for(size_t i = 0; i < num_pairs; i++){
    pthread_rwlock_unlock(&kvs_table->lockTable[hash(keys[i])]);
  }
}

int kvs_write(size_t num_pairs, char keys[][MAX_STRING_SIZE], char values[][MAX_STRING_SIZE]) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  /** Sort the keys. */
  mergeSort(keys, values, 0, num_pairs-1);
  /** Lock all of received inputs. */
  wrlock_table_entries(num_pairs, keys);

  for (size_t i = 0; i < num_pairs; i++) {
    if (write_pair(kvs_table, keys[i], values[i]) != 0) {
      fprintf(stderr, "Failed to write keypair (%s,%s)\n", keys[i], values[i]);
    }
  }

  /** Unlock all of received inputs. */
  unlock_table_entries(num_pairs, keys);
  return 0;
}

int kvs_read(size_t num_pairs, char keys[][MAX_STRING_SIZE], int fd) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }
  /** Sort the keys. */
  qsort(keys, num_pairs, sizeof(keys[0]), compare_keys);
  /** Lock all of received inputs. */
  rdlock_table_entries(num_pairs, keys);

  write(fd, "[", 1*sizeof(char));
  for (size_t i = 0; i < num_pairs; i++) {
    // strlen("(,KVSERROR)") = 11
    char buffer[MAX_STRING_SIZE + 11*sizeof(char) + 1];
    char* result = read_pair(kvs_table, keys[i]);

    if (result == NULL) {
      snprintf(buffer, sizeof(buffer), "(%s,KVSERROR)", keys[i]);
      /** strlen("(,KVSERROR)") = 11 */
      if(write_buffer(fd, buffer, strlen(keys[i]) + 11*sizeof(char)) == -1)
        fprintf(stderr, "Failed to write buffer on READ command.\n");
    } else {
      snprintf(buffer, sizeof(buffer), "(%s,%s)", keys[i], result);
      /** strlen("(,)") = 3. */
      if(write_buffer(fd, buffer,  strlen(keys[i]) + strlen(result) + 3*sizeof(char)) == -1)
        fprintf(stderr, "Failed to write buffer on READ command.\n");
    }
    free(result);
  }
  write(fd, "]\n", 2*sizeof(char));

  /** Unlock all of received inputs. */
  unlock_table_entries(num_pairs, keys);
  return 0;
}

int kvs_delete(size_t num_pairs, char keys[][MAX_STRING_SIZE], int fd) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }
  int aux = 0;
  
  /** Sort the keys. */
  qsort(keys, num_pairs, sizeof(keys[0]), compare_keys);
  
  /** Lock all of received inputs. */
  wrlock_table_entries(num_pairs, keys);

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

  /** Unlock all of received inputs. */
  unlock_table_entries(num_pairs, keys);
  return 0;
}

void kvs_show(int fd) {
  /** Lock the hashtable to read. */
  for (size_t i = 0; i < TABLE_SIZE; i++){
    pthread_rwlock_rdlock(&kvs_table->lockTable[i]);
  }

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

  /** Unlock the hashtable. */
  for (size_t i = 0; i < TABLE_SIZE; i++){
    pthread_rwlock_unlock(&kvs_table->lockTable[i]);
  }
}

/// Creates the path for a backup file and opens it.
/// @param file_name File name of the corresponding .job file. 
/// @param backup_number Number of the backup being done.
/// @return File Descriptor for backup file.
int create_backup_file(char file_name[], size_t backup_number){
  size_t length = strlen(file_name);
  /** Calculate length of backup_number. */
  size_t numsize = 0;
  for (size_t backup_number_copy = backup_number; backup_number_copy > 0; backup_number_copy/=10)
    numsize++;

  /** Create a buffer for sufix of file name.. */
  /** strlen("-.bck") = 5 */
  char buffer[numsize + 5*sizeof(char) + 1];
  buffer[numsize + 5*sizeof(char)] = '\0';
  if(snprintf(buffer, sizeof(buffer), "-%zd.bck", backup_number) == -1){
    fprintf(stderr, "Failed to create .bck extension on backup file.\n");
    return -1;
  }

  /** Create a "string" that can hold the whole backup file's name.*/
  char new_file_name[length - 4 + numsize + 5*sizeof(char) + 1]; 
  strncpy(new_file_name, file_name, length - 4);
  new_file_name[length - 4] = '\0';
  strncat(new_file_name, buffer, numsize + 5*sizeof(char)); 

  /** Open backup file. */
  int fd = open(new_file_name, O_CREAT | O_TRUNC | O_WRONLY, S_IRUSR | S_IWUSR);
  return fd;
}

int kvs_backup(char file_name[], size_t* backups_done, size_t *backups_left, pthread_mutex_t *backup_mutex){
  pid_t pid;
  int status;

  pthread_mutex_lock(backup_mutex);
  /** Wait until there's backups to do. */
  if(*backups_left == 0){
    /** Wait until a child process finishes. */
    wait(&status);
    /** There's a new backup that can be done. */
    (*backups_left)++;  
  }
  pthread_mutex_unlock(backup_mutex);

  /** Lock the hashtable to read. */
  for (size_t i = 0; i < TABLE_SIZE; i++){
    pthread_rwlock_rdlock(&kvs_table->lockTable[i]);
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
      fprintf(stderr, "Failure creating backup %zd for file \"%s\"\n", *backups_done, file_name);
      _exit(EXIT_FAILURE);
    }
    /** Copy info of KVS. */
    kvs_show(fd);
    close(fd);
    /** Exit out of child process. */
    _exit(EXIT_SUCCESS);
  }
  /** Parent. */
  else{
    /** Unlock the hashtable. */
    for (size_t i = 0; i < TABLE_SIZE; i++){
      pthread_rwlock_unlock(&kvs_table->lockTable[i]);
    }
    pthread_mutex_lock(backup_mutex);
    (*backups_left)--;
    pthread_mutex_unlock(backup_mutex);
    (*backups_done)++;
    return 0;
  }    
}

int subscribe_key(const char* key, const int notif_fd){
  int index = hash(key);
  KeyNode* keyNode = kvs_table->table[index];

  while (keyNode != NULL){
    if (strcmp(key, keyNode->key) == 0){
      pthread_rwlock_wrlock(&keyNode->client_list->lockList); 
      addClientId(keyNode->client_list, notif_fd);
      pthread_rwlock_unlock(&keyNode->client_list->lockList);
      return 0;
      }
      keyNode = keyNode->next;
    }

    return 1;
}

int unsubscribe_key(const char* key, const int notif_fd){
  int index = hash(key);
  KeyNode* keyNode = kvs_table->table[index];

  while (keyNode != NULL){
    if (strcmp(key, keyNode->key) == 0){
      pthread_rwlock_wrlock(&keyNode->client_list->lockList); 
      removeClientId(keyNode->client_list, notif_fd);
      pthread_rwlock_unlock(&keyNode->client_list->lockList);
      return 0;
      }

    keyNode = keyNode->next;
    }

    return 1;
}

void delete_all_subscriptions(int notif_fd){
  for(int i = 0; i < TABLE_SIZE; i++){
    KeyNode * keyNode = kvs_table->table[i];
    while(keyNode != NULL){
      List *client_list = keyNode->client_list;
      Node *aux = client_list->head;

      /** Remove if list isn't empty. */
      if(aux != NULL){
        /** Remove head edge case. */
        if(aux->notif_fd == notif_fd){
          Node *temp = aux;
          client_list->head = aux->next;
          free(temp);
          continue;
        }
        while(aux->next != NULL){
          if(aux->next->notif_fd == notif_fd){
            Node *temp = aux->next;
            aux->next = aux->next->next;
            free(temp);
          }
          aux = aux->next;
        }
      }
      keyNode = keyNode->next;
    }
  }
}

void kvs_wait(unsigned int delay_ms) {
  struct timespec delay = delay_to_timespec(delay_ms);
  nanosleep(&delay, NULL);
}