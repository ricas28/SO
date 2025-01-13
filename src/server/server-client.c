#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <semaphore.h>
#include <fcntl.h>
#include <signal.h>
#include <sys/select.h>

#include "src/common/constants.h"
#include "src/common/io.h"
#include "src/common/protocol.h"
#include "io.h"
#include "parser.h"
#include "constants.h"
#include "server-client.h"
#include "operations.h"

int _SIGSUSR1_received = 0;

Server_data *new_server_data(){
  Server_data *new_thread = (Server_data*)malloc(sizeof(Server_data));

  /** Allocate memory. */
  if(new_thread == NULL){
    fprintf(stderr, "Failure to allocate memory for new server_thread struct.\n");
    return NULL;
  }

  /** Initialize. */
  if(sem_init(&new_thread->empty, 0, BUFFER_SIZE) == -1){
    fprintf(stderr, "Failure to initalize empty semaphore.\n");
    destroy_server_data(new_thread);
    return NULL;
  }
  if(sem_init(&new_thread->full, 0, 0) == -1){
    fprintf(stderr, "Failure to initalize full semaphore.\n");
    destroy_server_data(new_thread);
    return NULL;
  }
  if(sem_init(&new_thread->active_sessions, 0, MAX_SESSION_COUNT)){
    fprintf(stderr, "Failure to initalize full semaphore.\n");
    destroy_server_data(new_thread);
    return NULL;
  }
  if(pthread_mutex_init(&new_thread->buffer_mutex, NULL) != 0){
    fprintf(stderr, "Failure to initialize buffer mutex.\n");
    destroy_server_data(new_thread);
    return NULL;
  }
  if(pthread_mutex_init(&new_thread->client_mutex, NULL) != 0){
    fprintf(stderr, "Failure to initialize client list mutex.\n");
    destroy_server_data(new_thread);
    return NULL;
  }

  new_thread->client_head = NULL;
  new_thread->write_Index = 0;
  new_thread->read_Index = 0;
  return new_thread;
}

void add_client(Client_Node *head, int resp_fd, int notif_fd){
  Client_Node *node = (Client_Node*)malloc(sizeof(Client_Node));

  node->resp_fd = resp_fd;
  node->notif_fd = notif_fd;
  node->next = head;
  head = node;
}

int equal_fds(Client_Node *node, int resp_fd, int notif_fd){
  if(node == NULL) return 0;

  return node->resp_fd == resp_fd &&
         node->notif_fd == notif_fd;
}

void remove_client(Client_Node *head, int resp_fd, int notif_fd){
  Client_Node *aux = head;

  if(head == NULL) return;

  /** Remove head. */
  if(equal_fds(head, resp_fd, notif_fd)){
    Client_Node *temp = head->next;
    free(head);
    head = temp;
  }
  /** Remove on the middle. */
  while(aux->next != NULL){
    if(equal_fds(aux->next, resp_fd, notif_fd)){
      Client_Node *temp = aux->next;
      aux->next = aux->next->next;
      free(temp);
      return;
    }
    aux = aux->next;
  }
}

void close_all_clients(Client_Node *head){
  while(head != NULL){
    Client_Node *temp = head->next;
    close(head->resp_fd);
    close(head->notif_fd);
    free(head);
    head = temp;
  }
}

void destroy_server_data(Server_data *server_data){
  if (server_data) {
    sem_destroy(&server_data->empty);
    sem_destroy(&server_data->full);
    sem_destroy(&server_data->active_sessions);
    pthread_mutex_destroy(&server_data->buffer_mutex);
    pthread_mutex_destroy(&server_data->client_mutex);
    free(server_data);
  }
}

char* consume_request(Server_data *server_data){
  char* request_message = (char*)malloc(MAX_REGISTER_MSG*sizeof(char));
  
  sem_wait(&server_data->full); 
  pthread_mutex_lock(&server_data->buffer_mutex); 
  memcpy(request_message, server_data->buffer[server_data->read_Index], MAX_REGISTER_MSG);
  server_data->read_Index = (server_data->read_Index + 1) % BUFFER_SIZE;
  pthread_mutex_unlock(&server_data->buffer_mutex); 
  sem_post(&server_data->empty); 

  return request_message;
}

void produce_request(Server_data *server_data, char *message){
  /** Wait until there's space to put message. */
  sem_wait(&server_data->empty);
  pthread_mutex_lock(&server_data->buffer_mutex);
  memcpy(server_data->buffer[server_data->write_Index], message, MAX_REGISTER_MSG);
  server_data->write_Index = (server_data->write_Index + 1) % BUFFER_SIZE;
  pthread_mutex_unlock(&server_data->buffer_mutex);
  /** Signal that there's a new mensage to read. */
  sem_post(&server_data->full);
}

int open_pipes(int *req_fd, int *resp_fd, int *notif_fd,
          const char *req_pipe, const char *resp_pipe, const char *notif_pipe){
  /** Open response pipe. */
  if((*resp_fd = open(resp_pipe, O_WRONLY)) == -1){
    fprintf(stderr, "Failure to open response pipe.");
    return 1;
  }

  /** Connect was successful. */
  if(write_all(*resp_fd, "10", 2) == -1){
    fprintf(stderr, "Failure to write connect mensage.");
    close(*resp_fd);
    return 1;
  }

  /** Open request pipe. */
  if((*req_fd = open(req_pipe, O_RDONLY)) == -1){
    fprintf(stderr, "Failure to open request pipe.\n");
    close(*resp_fd);
    return 1;
  }

  /** Open notification pipe. */
  if((*notif_fd = open(notif_pipe, O_WRONLY)) == -1){
    fprintf(stderr, "Failure to open notification pipe.\n");
    close(*resp_fd);
    close(*req_fd);
    return 1;
  }

  return 0;
}

void client_disconnect(int req_fd, int resp_fd, int notif_fd, Server_data *server_data, int *connected){
  close(req_fd);
  close(resp_fd);
  close(notif_fd);
  sem_post(&server_data->active_sessions);
  pthread_mutex_lock(&server_data->client_mutex);
  remove_client(server_data->client_head, resp_fd, notif_fd);
  pthread_mutex_unlock(&server_data->client_mutex);
  *connected = 0;
}

void* managing_thread_fn(void *arg){
  Server_data *server_data = (Server_data*) arg;
  int req_fd, resp_fd, notif_fd, error = 0, connected = 0;
  char *connect_message;
  char req_pipe[MAX_PIPE_PATH_LENGTH];
  char resp_pipe[MAX_PIPE_PATH_LENGTH];
  char notif_pipe[MAX_PIPE_PATH_LENGTH];
  sigset_t mask;

  sigemptyset (&mask);
  sigaddset (&mask, SIGUSR1);
  pthread_sigmask(SIG_BLOCK, &mask, NULL);

  while(error == 0){
    error = 0;
    /** Consume a connect request. */
    connect_message = consume_request(server_data);
    strncpy(req_pipe, connect_message+1, MAX_PIPE_PATH_LENGTH);
    strncpy(resp_pipe, connect_message+MAX_PIPE_PATH_LENGTH + 1, MAX_PIPE_PATH_LENGTH);
    strncpy(notif_pipe, connect_message+MAX_PIPE_PATH_LENGTH*2 + 1, MAX_PIPE_PATH_LENGTH);
    free(connect_message);
    connected = 1;

    if(open_pipes(&req_fd, &resp_fd, &notif_fd, req_pipe, resp_pipe, notif_pipe)) return NULL;

    /** Add client to current clients list. */
    pthread_mutex_lock(&server_data->client_mutex);
    add_client(server_data->client_head, resp_fd, notif_fd);
    pthread_mutex_unlock(&server_data->client_mutex);

    while(error == 0 && connected){
      char request_message[MAX_REGISTER_MSG]; 
      /** Read OP CODE. */
      ssize_t ret = read_all(req_fd, request_message, 1, NULL);
      /** Client sudden disconnect. */
      if(ret == 0 && errno == 0){
        client_disconnect(req_fd, resp_fd, notif_fd, server_data, &connected);
        break;
      }
      switch (request_message[0]) {
        case OP_CODE_DISCONNECT:
          delete_client_subscriptions(notif_fd);
          if(write_all(resp_fd, "20", 2) == -1){
            if(errno != EBADF){
              fprintf(stderr, "Failure to write disconnect mensage (success)\n");
              error = 1;
            }
            break;
          }
          client_disconnect(req_fd, resp_fd, notif_fd, server_data, &connected);
          break;

        case OP_CODE_SUBSCRIBE:
          if(read_all(req_fd, request_message + 1, MAX_STRING_SIZE + 1, NULL) == -1){
            fprintf(stderr, "Failure to read subsribe request.\n");
            error = 1;
          }
          if(subscribe_key(request_message + 1, notif_fd)){
            /** Key was not found. */
            if(write_all(resp_fd, "30", 2) == -1){
              if(errno != EBADF){
                fprintf(stderr, "Failure to write subscribe (Key not found).\n");
                error = 1;
              }
            }
          }
          else{
            if(write_all(resp_fd, "31", 2) == -1){
              if(errno != EBADF){
                fprintf(stderr, "Failure to write subscribe (success).\n");
                error = 1;
              }
            }
          }
          break;

        case OP_CODE_UNSUBSCRIBE:
          if(read_all(req_fd, request_message + 1, MAX_STRING_SIZE + 1, NULL) == -1){
            fprintf(stderr, "Failure to read subsribe request.\n");
            error = 1;
          }
          if(unsubscribe_key(request_message + 1, notif_fd)){
            if(write_all(resp_fd, "41", 2) == -1){
              if(errno != EBADF){
                fprintf(stderr, "Failure to write unsubscribe (subscription not found)\n");
                error = 1;
              }
            }
          }
          else{
            if(write_all(resp_fd, "40", 2) == -1){
              if(errno != EBADF){
                fprintf(stderr, "Failure to unsubscribe (success).\n");
                error = 1;
              }
            }
          }
          break;

        default:
          printf("Strange OP.\n");
          break;
      }
    }
  }

  client_disconnect(req_fd, resp_fd, notif_fd, server_data, &connected);
  return NULL;
}

void handle_SIGUSR1(int signum){
  (void)signum; /** To supress warning. */
  printf("Recebi\n");
  _SIGSUSR1_received = 1;
}

void* host_thread_fn(void* arg){
  Host_thread *host_thread = (Host_thread*) arg;
  int fifo_fd;

  /** Handle SIGUSR1. */
  signal(SIGUSR1, handle_SIGUSR1);

  /** Open register FIFO for reading */
  fifo_fd = open(host_thread->register_FIFO, O_RDONLY); 
  if (fifo_fd == -1) {
    fprintf(stderr, "Failure opening FIFO.\n");
    return NULL;
  }
  
  while (1) {
    char *buffer = (char*) calloc(MAX_REGISTER_MSG, sizeof(char));
    ssize_t ret;
    int intr = 0;

    if((ret =  read_all(fifo_fd, buffer, MAX_REGISTER_MSG, &intr)) == -1 && intr == 0){
      fprintf(stderr, "Failure reading from register FIFO.\n");
      free(buffer);
      break;
    }

    if(_SIGSUSR1_received && intr){
      delete_all_subscriptions();
      pthread_mutex_lock(&host_thread->server_data->client_mutex);
      close_all_clients(host_thread->server_data->client_head);
      pthread_mutex_unlock(&host_thread->server_data->client_mutex);
      _SIGSUSR1_received = 0;
    }
  	
    if(ret != 0){
      /** Wait until a client can join. */
      sem_wait(&host_thread->server_data->active_sessions);
      /** Nothing useful was read. */
      if (ret == 1 && (buffer[0] == '\n')) { 
        free(buffer);
        continue;
      }

      /** Put connect request on request buffer. */
      produce_request(host_thread->server_data, buffer);
    }
    else if (errno != 0){
      fprintf(stderr, "FIFO broken.\n");
      free(buffer);
      break;
    }
    free(buffer);
  }
  
  close(fifo_fd);
  return NULL;
}