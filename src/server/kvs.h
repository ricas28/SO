#ifndef KEY_VALUE_STORE_H
#define KEY_VALUE_STORE_H

#define TABLE_SIZE 26

#include <stddef.h>
#include <pthread.h>

typedef struct KeyNode {
    char *key;
    char *value;
    struct KeyNode *next;
    struct List* client_list;
} KeyNode;

typedef struct HashTable {
    KeyNode *table[TABLE_SIZE];
    pthread_rwlock_t lockTable[TABLE_SIZE];
} HashTable;

typedef struct Node {
    int notif_fd;
    struct Node* next;
} Node;

typedef struct List{
    struct Node* head;
    pthread_rwlock_t lockList;
} List;

// Hash function based on key initial.
// @param key Lowercase alphabetical string.
// @return hash.
// NOTE: This is not an ideal hash function, but is useful for test purposes of the project
int hash(const char *key);

/// Creates a new event hash table.
/// @return Newly created hash table, NULL on failure
struct HashTable *create_hash_table();

/// Appends a new key value pair to the hash table.
/// @param ht Hash table to be modified.
/// @param key Key of the pair to be written.
/// @param value Value of the pair to be written.
/// @return 0 if the node was appended successfully, 1 otherwise.
int write_pair(HashTable *ht, const char *key, const char *value);

/// Frees the client list.
/// @param list 
void freeList(List* list);

/// Adds the client to the list of clients subscribed to the key.
/// @param client_list List of the client notifcation fd's.
/// @param notif_fd notification fd of the client.
void addClientId(List* client_list, const int notif_fd);

/// Removes the client from the list of clients subscribed to the key.
/// @param client_list List of the client notifcation fd's.
/// @param notif_fd notification fd of the client.
/// @return 0 if notif_fd was removed, 1 if subscription didin't exist.
int removeClientId(List* client_list, const int notif_fd);

/// Deletes the value of given key.
/// @param ht Hash table to delete from.
/// @param key Key of the pair to be deleted.
/// @return 0 if the node was deleted successfully, 1 otherwise.
char* read_pair(HashTable *ht, const char *key);

/// Appends a new node to the list.
/// @param list Event list to be modified.
/// @param key Key of the pair to read.
/// @return 0 if the node was appended successfully, 1 otherwise.
int delete_pair(HashTable *ht, const char *key);

/// Frees the hashtable.
/// @param ht Hash table to be deleted.
void free_table(HashTable *ht);

/// Free all client nodes of a list.
/// @param list 
void freeClientNodes(List* list);

#endif  // KVS_H
