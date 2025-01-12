#include "kvs.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <pthread.h>

#include "constants.h"
#include "src/common/io.h"


int hash(const char *key) {
    int firstLetter = tolower(key[0]);
    if (firstLetter >= 'a' && firstLetter <= 'z') {
        return firstLetter - 'a';
    } else if (firstLetter >= '0' && firstLetter <= '9') {
        return firstLetter - '0';
    }
    return -1; // Invalid index for non-alphabetic or number strings
}


struct HashTable* create_hash_table() {
  HashTable *ht = malloc(sizeof(HashTable));
  if (!ht) return NULL;
  for (int i = 0; i < TABLE_SIZE; i++) {
      ht->table[i] = NULL;
      pthread_rwlock_init(&ht->lockTable[i], NULL); // initiate rwlocks.
  }
  return ht;
}

void notify_key_change(KeyNode *node){
    Node *aux = node->client_list->head;
    size_t key_len = strlen(node->key), value_len = strlen(node->value);
    /** 3 for "(,)" and 2 for the two '\0'. */
    char buffer[MAX_STRING_SIZE*2 + 3 + 2];

    sprintf(buffer, "(%s", node->key);
    for(size_t i = key_len + 2; i <= MAX_STRING_SIZE + 1; i++){
        buffer[i] = ' ';
    }
    sprintf(buffer + MAX_STRING_SIZE + 2, ",%s", node->value);
    /** Start at buffer[1+ MAX_STRING_SIZE + 1 + 1 + value_len + 1] */
    for(size_t i = MAX_STRING_SIZE + value_len + 3 ; i <= MAX_STRING_SIZE*2 + 3; i++){
        buffer[i] = ' ';
    }
    buffer[MAX_STRING_SIZE*2 + 4] = ')';

    while(aux != NULL){
        write_all(aux->notif_fd, buffer, MAX_STRING_SIZE*2 + 5);
        aux = aux->next;
    }
}


void notify_key_deletion(KeyNode *node){
    Node *aux = node->client_list->head;
    size_t key_len = strlen(node->key);
    /** 3 for "(,)" and 2 for the two '\0'. */
    char buffer[MAX_STRING_SIZE*2 + 3 + 2];

    sprintf(buffer, "(%s", node->key);
    for(size_t i = key_len + 2; i <= MAX_STRING_SIZE + 1; i++){
        buffer[i] = ' ';
    }
    sprintf(buffer + MAX_STRING_SIZE + 2, ",DELETED");
    /** Start at buffer[1+ MAX_STRING_SIZE + 1 + 7 + 1] strlen("DELETED") = 7 */
    for(size_t i = MAX_STRING_SIZE + 7 + 3 ; i <= MAX_STRING_SIZE*2 + 3; i++){
        buffer[i] = ' ';
    }
    buffer[MAX_STRING_SIZE*2 + 4] = ')';

    while(aux != NULL){
        write_all(aux->notif_fd, buffer, MAX_STRING_SIZE*2 + 5);
        aux = aux->next;
    }
}

int write_pair(HashTable *ht, const char *key, const char *value) {
    int index = hash(key);
    KeyNode *keyNode = ht->table[index];
    // Search for the key node
    while (keyNode != NULL) {
        if (strcmp(keyNode->key, key) == 0) {    
            free(keyNode->value);
            keyNode->value = strdup(value);
            /** A change on the key occured. */
            notify_key_change(keyNode);
            return 0;
        }
        keyNode = keyNode->next; // Move to the next node
    }

    // Key not found, create a new key node
    keyNode = (KeyNode*)malloc(sizeof(KeyNode));
    keyNode->client_list = (List*)malloc(sizeof(List));
    keyNode->client_list->head = NULL;
    pthread_rwlock_init(&keyNode->client_list->lockList, NULL); // Lock for each notif_fd list
    keyNode->key = strdup(key); // Allocate memory for the key
    keyNode->value = strdup(value); // Allocate memory for the value
    keyNode->next = ht->table[index]; // Link to existing nodes
    ht->table[index] = keyNode; // Place new key node at the start of the list
    return 0;
}

char* read_pair(HashTable *ht, const char *key) {
    int index = hash(key);
    KeyNode *keyNode = ht->table[index];
    char* value;

    while (keyNode != NULL) {
        if (strcmp(keyNode->key, key) == 0) {
            value = strdup(keyNode->value);
            return value; // Return copy of the value if found
        }
        keyNode = keyNode->next; // Move to the next node
    }
    return NULL; // Key not found
}

int delete_pair(HashTable *ht, const char *key) {
    int index = hash(key);
    KeyNode *keyNode = ht->table[index];
    KeyNode *prevNode = NULL;

    // Search for the key node
    while (keyNode != NULL) {
        if (strcmp(keyNode->key, key) == 0) {
            // Key found; delete this node
            if (prevNode == NULL) {
                // Node to delete is the first node in the list
                ht->table[index] = keyNode->next; // Update the table to point to the next node
            } else {
                // Node to delete is not the first; bypass it
                prevNode->next = keyNode->next; // Link the previous node to the next node
            }
            // Notify clients of deletion.
            notify_key_deletion(keyNode);
            // Free client list.
            freeList(keyNode->client_list); 
            // Free the memory allocated for the key and value
            free(keyNode->key);
            free(keyNode->value);
            free(keyNode); // Free the key node itself
            return 0; // Exit the function
        }
        prevNode = keyNode; // Move prevNode to current node
        keyNode = keyNode->next; // Move to the next node
    }
    
    return 1;
}

void freeList(List* list){
    Node* tmp = list->head;
    Node* prev = NULL;

    if(list->head == NULL) return;
    while(tmp != NULL){
        prev = tmp;
        tmp = tmp->next;
        free(prev);
    }
    free(list);
}

void addClientId(List* client_list, const int notif_fd){
    Node* newNode = (Node*)malloc(sizeof(Node));
    newNode->notif_fd = notif_fd;

    if (client_list->head == NULL){
        client_list->head = newNode;
        client_list->head->next = NULL;
        return;
    }
    newNode->next = client_list->head;
    client_list->head = newNode;
}

int removeClientId(List* client_list, const int notif_fd){
    Node *aux = client_list->head;

    /** Client_list is empty. */
    if(client_list->head == NULL) return 1;

    /** Remove head. */
    if(client_list->head->notif_fd == notif_fd){
        Node *temp = client_list->head;
        client_list->head = client_list->head->next;
        free(temp);
        return 0;
    }
    while(aux->next != NULL){
        if(aux->next->notif_fd == notif_fd){
          Node *temp = aux->next;
          aux->next = aux->next->next;
          free(temp);
          return 0;
        }
        aux = aux->next;
    }
    /** notif_fd was not found. */
    return 1;
}

void free_table(HashTable *ht) {
    for (int i = 0; i < TABLE_SIZE; i++) {
        KeyNode *keyNode = ht->table[i];
        while (keyNode != NULL) {
            KeyNode *temp = keyNode;
            keyNode = keyNode->next;
            freeList(temp->client_list);
            free(temp->key);
            free(temp->value);
            free(temp);
        }
    }
    free(ht);
}