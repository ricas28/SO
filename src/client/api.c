#include "api.h"
#include <stdlib.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>

#include "src/common/constants.h"
#include "src/common/protocol.h"
#include "src/common/io.h"

char REQ_PIPE_PATH[256];
char RESP_PIPE_PATH[256];
char NOTIF_PIPE_PATH[256];
char SERVER_PIPE_PATH[256];

int kvs_connect(int *req_fd, int *resp_fd, int *notif_fd, const char* req_pipe_path, 
                const char *resp_pipe_path, const char *notif_pipe_path, const char *server_pipe_path) {
  size_t req_pipe_size;
  size_t resp_pipe_size;
  size_t notif_pipe_size;

  strcpy(REQ_PIPE_PATH, req_pipe_path);
  strcpy(RESP_PIPE_PATH, resp_pipe_path);
  strcpy(NOTIF_PIPE_PATH ,notif_pipe_path);
  strcpy(SERVER_PIPE_PATH, server_pipe_path);

  /** Erase previous FIFOs*/
  unlink(req_pipe_path);
  unlink(resp_pipe_path);
  unlink(notif_pipe_path);

  /* Create FIFOs. */
  if (mkfifo(req_pipe_path, 0666) != 0){fprintf(stderr, "ERROR: Creating request pipe.\n"); return 1;}
  if (mkfifo(resp_pipe_path, 0666) != 0){fprintf(stderr, "ERROR: Creating response pipe.\n"); return 1;}
  if (mkfifo(notif_pipe_path, 0666) != 0){fprintf(stderr, "ERROR: Creating notifications pipe.\n"); return 1;}

  /* Send the message through the server pipe. */
  req_pipe_size = strlen(req_pipe_path);
  resp_pipe_size = strlen(resp_pipe_path);
  notif_pipe_size = strlen(notif_pipe_path);

  /** Create request mensage. */
  char buffer[MAX_PIPE_PATH_LENGTH*3 + 1]; //buffer to store the path names and the opcode 1.
  snprintf(buffer, MAX_PIPE_PATH_LENGTH + 1, "%c%s", OP_CODE_CONNECT, req_pipe_path);
  for(size_t i = req_pipe_size + 1; i <= MAX_PIPE_PATH_LENGTH; i++)
    buffer[i] = '\0';
  strncpy(buffer + MAX_PIPE_PATH_LENGTH + 1, resp_pipe_path, resp_pipe_size);
  for(size_t i = MAX_PIPE_PATH_LENGTH + resp_pipe_size + 1; i <= MAX_PIPE_PATH_LENGTH*2; i++)
    buffer[i] = '\0';
  strncpy(buffer + MAX_PIPE_PATH_LENGTH*2 + 1, notif_pipe_path, notif_pipe_size);
  for(size_t i = MAX_PIPE_PATH_LENGTH*2 + notif_pipe_size + 1; i <= MAX_PIPE_PATH_LENGTH*3; i++)
    buffer[i] = '\0';

  /** Open server pipe and send the message. */
  int server_fd = open(server_pipe_path, O_WRONLY);
  if(write_all(server_fd, buffer, sizeof(buffer)) == -1){
    fprintf(stderr, "Failure writing request for connect.\n");
    return 1;
  }

  char result_message[2];
  /** Open response FIFO. */
  if((*resp_fd = open(resp_pipe_path, O_RDONLY)) == -1){
    fprintf(stderr, "Failure to open response pipe.\n");
    return 1;
  }
  /** Open request FIFO. */
  if((*req_fd = open(req_pipe_path, O_WRONLY)) == -1){
    fprintf(stderr, "Failure to open request pipe.\n");
    close(*resp_fd);
    return 1;
  }
  /** Open notification FIFO. */
  if((*notif_fd = open(notif_pipe_path, O_RDONLY)) == -1){
    fprintf(stderr, "Failure to open notification pipe.\n");
    close(*resp_fd);
    close(*req_fd);
    return 1;
  }
  /** Read result mensage. */
  if(read_all(*resp_fd, result_message, 2, NULL) == -1){
    fprintf(stderr, "Failure reading result mensage for connect.\n");
    close(*resp_fd);
    close(*req_fd);
    close(*notif_fd);
    return 1;
  }

  printf("Server returned %c for operation: connect\n", result_message[1]);
  return 0;
}

int kvs_disconnect(void) {
  // close pipes and unlink pipe files
  return 0;
}

int kvs_subscribe(int req_fd, int resp_fd, const char *key) {
  char result_message[2];
  char send_message[MAX_STRING_SIZE + 2];
  size_t key_size = strlen(key);
  
  /* Build the string with the message to send (Opcode 3 + key). */
  send_message[0] = OP_CODE_SUBSCRIBE;
  strcat(send_message, key);
  /** Add padding. */
  for(size_t i = key_size + 1; i < MAX_STRING_SIZE + 2; i++){
    send_message[i] = '\0';
  }

  /* Write the key meant to subscribe into the request pipe. */
  if(write_all(req_fd, send_message, MAX_STRING_SIZE + 2) == -1){
    fprintf(stderr, "ERROR: Failure writing (the key) into the request pipe.\n");
    return 1;
  }

  /* Read the response from the response pipe. */
  if(read_all(resp_fd, result_message, 2, NULL) == -1){
    fprintf(stderr, "ERROR: Failure reading from the response pipe.\n");
    return 1;
  }

  printf("Server returned %c for operation: subscribe\n", result_message[1]);
  return 0;
}

int kvs_unsubscribe(int req_fd, int resp_fd, const char *key) {
  char result_message[2];
  char send_message[MAX_STRING_SIZE + 2];
  size_t key_size = strlen(key);

  /* Build the string with the message to send (OP_CODE_UNSUBSCRIBE + key). */
  send_message[0] = OP_CODE_UNSUBSCRIBE;
  strcat(send_message, key);
  /** Add padding. */
  for(size_t i = key_size + 1; i < MAX_STRING_SIZE + 2; i++){
    send_message[i] = '\0';
  }

  /* Write the key meant to subscribe into the request pipe. */
  if(write_all(req_fd, send_message, MAX_STRING_SIZE+2) == -1){
    fprintf(stderr, "ERROR: Failure writing (the key) into the request pipe.\n");
    return 1;
  }

  /* Read the response from the response pipe. */
  if(read_all(resp_fd, result_message, 2, NULL) == -1){
    fprintf(stderr, "ERROR: Failure reading from the response pipe.\n");
    return 1;
  }

  printf("Server returned %c for operation: unsubscribe\n", result_message[1]);
  return 0;
}

void* notifications_manager(void *arg){
  int *notif_fd = (int*) arg;
  ssize_t read;
  /** 3 for "(,)" and 2 for the two '\0'. */
  char buffer[MAX_STRING_SIZE*2 + 3 + 2];

  while(1){
    if((read = read_all(*notif_fd, buffer, MAX_STRING_SIZE*2 + 3, NULL)) == -1){
      fprintf(stderr, "Failure to read from notification pipe.\n");
      break;
    }

    if(read == 0){
      fprintf(stderr, "PIPE broken.\n");
      break;
    }
    for(int i = 0; i < MAX_STRING_SIZE*2 + 3 + 2; i++){
      if(buffer[i] != ' '){
        printf("%c", buffer[i]);
      }
    }
    printf("\n");
  }
  return NULL;
}
