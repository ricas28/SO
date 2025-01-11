#ifndef CLIENT_API_H
#define CLIENT_API_H

#include <stddef.h>
#include <pthread.h>

#include "src/common/constants.h"

/// Connects to a kvs server.
/// @param req_pipe_path Path to the name pipe to be created for requests.
/// @param resp_pipe_path Path to the name pipe to be created for responses.
/// @param server_pipe_path Path to the name pipe where the server is listening.
/// @return 0 if the connection was established successfully, 1 otherwise.
int kvs_connect(int *req_fd, int *resp_fd, int *notif_fd, const char* req_pipe_path, 
                const char *resp_pipe_path, const char *notif_pipe_path, const char *server_pipe_path);
/// Disconnects from an KVS server.
/// @return 0 in case of success, 1 otherwise.
int kvs_disconnect(int req_fd, int resp_fd);

/// Requests a subscription for a key
/// @param key Key to be subscribed
/// @return 1 if the key was subscribed successfully (key existing), 0
/// otherwise.

int kvs_subscribe(int req_fd, int resp_fd, const char *key);

/// Remove a subscription for a key
/// @param key Key to be unsubscribed
/// @return 0 if the key was unsubscribed successfully  (subscription existed
/// and was removed), 1 otherwise.

int kvs_unsubscribe(int req_fd, int resp_fd, const char *key);

/// Thread open for the notifications pipe.  
void* notifications_manager();

/// Joins the notifications thread to the main thread and destroys it's pipes.
/// @param notif_fd 
/// @param notif_thread 
/// @return 0 if successfull.
int end_notifications_thread(int notif_fd, pthread_t notif_thread);

#endif // CLIENT_API_H
