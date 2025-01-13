#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "parser.h"
#include "src/client/api.h"
#include "src/common/constants.h"
#include "src/common/io.h"

int main(int argc, char *argv[]) {
  pthread_t notifications_thread;
  int req_fd, resp_fd, notif_fd, server_fd; 
  int res;

  if (argc < 3) {
    fprintf(stderr, "Usage: %s <client_unique_id> <register_pipe_path>\n", argv[0]);
    return 1;
  }

  char req_pipe_path[256] = "/tmp/req-group31-";
  char resp_pipe_path[256] = "/tmp/resp-group31-";
  char notif_pipe_path[256] = "/tmp/notif-group31-";

  char keys[MAX_NUMBER_SUB][MAX_STRING_SIZE] = {0};
  unsigned int delay_ms;
  size_t num;

  strncat(req_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));
  strncat(resp_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));
  strncat(notif_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));

  if(kvs_connect(&req_fd, &resp_fd, &notif_fd, &server_fd, req_pipe_path, resp_pipe_path, notif_pipe_path, argv[2]) == 1)
    return 1;
  
  if (pthread_create(&notifications_thread, NULL, notifications_manager, (void*)&notif_fd) != 0){
    fprintf(stderr, "ERROR: Unable to create notifications thread.\n");
    return 1;
  }

  while (1) {
    switch (get_next(STDIN_FILENO)) {
    case CMD_DISCONNECT:
      if ((res = kvs_disconnect(server_fd, req_pipe_path, resp_pipe_path)) == 1) {
        fprintf(stderr, "Failed to disconnect to the server.\n");
        return 1;
      }
      else if (res == 2){
        if (server_disconnected(server_fd, req_pipe_path, resp_pipe_path) != 0){
          fprintf(stderr, "Failed to close and unlink pipes.\n");
        }
        if (end_notifications_thread(notif_pipe_path, notifications_thread) != 0) {
          fprintf(stderr, "Failed to end notifications thread.\n");
          return 1;
        }
        printf("Server terminated the connection.\n");
      }
      
      printf("Disconnected from server.\n");
      return 0;

    case CMD_SUBSCRIBE:
      num = parse_list(STDIN_FILENO, keys, 1, MAX_STRING_SIZE);
      if (num == 0) {
        fprintf(stderr, "Invalid command. See HELP for usage\n");
        continue;
      }
      if ((res = kvs_subscribe(keys[0])) == 1) {
        fprintf(stderr, "Command subscribe failed\n");
      }
      else if (res == 2){
        if (server_disconnected(server_fd, req_pipe_path, resp_pipe_path) != 0){
          fprintf(stderr, "Failed to close and unlink pipes.\n");
        }
        if (end_notifications_thread(notif_pipe_path, notifications_thread) != 0) {
          fprintf(stderr, "Failed to end notifications thread.\n");
          return 1;
        }
        printf("Server terminated the conection.\n");
      }
      break;

    case CMD_UNSUBSCRIBE:
      num = parse_list(STDIN_FILENO, keys, 1, MAX_STRING_SIZE);
      if (num == 0) {
        fprintf(stderr, "Invalid command. See HELP for usage\n");
        continue;
      }
      if ((res = kvs_unsubscribe(keys[0])) == 1) {
        fprintf(stderr, "Command unsubscribe failed.\n");
      }
      else if (res == 2){
        if (server_disconnected(server_fd, req_pipe_path, resp_pipe_path) != 0){
          fprintf(stderr, "Failed to close and unlink pipes.\n");
        }
        if (end_notifications_thread(notif_pipe_path, notifications_thread) != 0) {
          fprintf(stderr, "Failed to end notifications thread.\n");
          return 1;
        }
        printf("Server terminated the conection.\n");
        return 1;
      }
      break;

    case CMD_DELAY:
      if (parse_delay(STDIN_FILENO, &delay_ms) == -1) {
        fprintf(stderr, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (delay_ms > 0) {
        printf("Waiting...\n");
        delay(delay_ms);
      }
      break;

    case CMD_INVALID:
      fprintf(stderr, "Invalid command. See HELP for usage\n");
      break;

    case CMD_EMPTY:
      break;

    case EOC:
      // input should end in a disconnect, or it will loop here forever
      break;
    }
  }

  return 0;
}
