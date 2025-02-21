#ifndef KVS_OPERATIONS_H
#define KVS_OPERATIONS_H

#include <stddef.h>

/// Compares two keys.
/// @param key1 
/// @param key2 
/// @return 0 if keys are equal.
int compare_keys(const void* key1, const void* key2);

/// Merges two arrays.
/// @param keys
/// @param values
/// @param l
/// @param m
/// @param r
void merge(char keys[][MAX_STRING_SIZE], char values[][MAX_STRING_SIZE], size_t l, size_t m, size_t r);

/// l is for left index and r is right index of the
/// sub-array of arr to be sorted.
/// @param keys
/// @param values
/// @param l 
/// @param r
void mergeSort(char keys[][MAX_STRING_SIZE], char values[][MAX_STRING_SIZE], size_t l, size_t r);

/// Lock all table entries with a write lock.
/// @param num_pairs 
/// @param keys
void wrlock_table_entries(size_t num_pairs, char keys[][MAX_STRING_SIZE]);

/// Lock all table entries with a read lock.
/// @param num_pairs 
/// @param keys
void rdlock_table_entries(size_t num_pairs, char keys[][MAX_STRING_SIZE]);

/// Initializes the KVS state.
/// @return 0 if the KVS state was initialized successfully, 1 otherwise.
int kvs_init();

/// Destroys the KVS state.
/// @return 0 if the KVS state was terminated successfully, 1 otherwise.
int kvs_terminate();

/// Writes a key value pair to the KVS. If key already exists it is updated.
/// @param num_pairs Number of pairs being written.
/// @param keys Array of keys' strings.
/// @param values Array of values' strings.
/// @return 0 if the pairs were written successfully, 1 otherwise.
int kvs_write(size_t num_pairs, char keys[][MAX_STRING_SIZE], char values[][MAX_STRING_SIZE]);

/// Reads values from the KVS.
/// @param num_pairs Number of pairs to read.
/// @param keys Array of keys' strings.
/// @param fd File descriptor to write the (successful) output.
/// @return 0 if the key reading, 1 otherwise.
int kvs_read(size_t num_pairs, char keys[][MAX_STRING_SIZE], int fd);

/// Deletes key value pairs from the KVS.
/// @param num_pairs Number of pairs to read.
/// @param keys Array of keys' strings.
/// @param fd File descriptor to write 
/// @return 0 if the pairs were deleted successfully, 1 otherwise.
int kvs_delete(size_t num_pairs, char keys[][MAX_STRING_SIZE], int fd);

/// Writes the state of the KVS.
/// @param fd File descriptor to write the output.
void kvs_show(int fd);

/// Opens a backup file and returns it's file descriptor.
/// @param file_name Name of the .job file.
/// @param backup_number Number of the backup being done.
/// @return File descriptor for new backup file.
int create_backup_file(char file_name[], size_t backup_number);

/// Creates a backup of the KVS state and stores it in the correspondent
/// backup file
/// @param file_name File of the .job file that is executing a backup.
/// @param backups_done pointer to number of backups the file has.
/// @param backups_left pointer to number of backups left the KVS can do at 
/// the moment.
/// @return 0 if the backup was successful, 1 otherwise.
int kvs_backup(char file_name[], size_t* backups_done, size_t *backups_left, pthread_mutex_t *backup_mutex);

/// Subscribes a client to the given key.
/// @param key Key of the pair to be subscribed.
/// @param notif_fd Fd of the client's notification FIFO.
/// @return 0 if successfull, 1 otherwise.
int subscribe_key(const char* key, const int notif_fd);

/// Unsubscribes a client to the given key.
/// @param key Key of the pair to be unsubscribed.
/// @param notif_fd Fd of the client's notification FIFO.
/// @return 0 if successfull, 1 otherwise.
int unsubscribe_key(const char* key, const int notif_fd);

/// Deletes every subscription of a client.
/// @param notif_fd Fd of the client's notification FIFO.
void delete_client_subscriptions(int notif_fd);

/// Deletes every subscription on the KVS server.
void delete_all_subscriptions();

/// Waits for a given amount of time.
/// @param delay_us Delay in milliseconds.
void kvs_wait(unsigned int delay_ms);

/// Writes everything that the buffer has to the file indicated by fd.
/// @param fd File descriptor to write output.
/// @param buffer Buffer with mensage to write.
/// @param buffer_size Size of the buffer.
/// @return 0 if the write was successful, -1 otherwise.
int write_buffer(int fd, char *buffer, size_t buffer_size);

#endif  // KVS_OPERATIONS_H
