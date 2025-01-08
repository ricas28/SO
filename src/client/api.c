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

int kvs_connect(const char* req_pipe_path, const char *resp_pipe_path,
                const char *notif_pipe_path, const char *server_pipe_path) {
  size_t req_pipe_size;
  size_t resp_pipe_size;
  size_t notif_pipe_size;

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
  snprintf(buffer, MAX_PIPE_PATH_LENGTH + 1, "1%s", req_pipe_path);
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

  int intr, resp_pipe;
  char result_mensage[2];
  /** Open responde FIFO. */
  if((resp_pipe = open(resp_pipe_path, O_RDONLY)) == -1){
    fprintf(stderr, "Failure opening responde pipe for connect.\n");
    return 1;
  }
  /** Read result mensage. */
  if(read_all(resp_pipe, result_mensage, 2, &intr) == -1){
    fprintf(stderr, "Failure reading result mensage for connect.\n");
    return 1;
  }
  printf("Server returned %c for operation: connect\n", result_mensage[1]);
  return 0;
}

int kvs_disconnect(void) {
  // close pipes and unlink pipe files
  return 0;
}

int kvs_subscribe(const char *key) {
  fprintf(stderr, "%s", key); // Just to compile.
  // send subscribe message to request pipe and wait for response in response
  // pipe
  return 0;
}

int kvs_unsubscribe(const char *key) {
  fprintf(stderr, "%s", key); // Just to compile.
  // send unsubscribe message to request pipe and wait for response in response
  // pipe
  return 0;
}

void* notifications_manager(void* arg){
  const char* notif_pipe_path = (char *) arg;
  int notif_fd; 
  ssize_t status = 1;
  char buffer[MAX_STRING_SIZE];

  // Open the notifications pipe.
  if ((notif_fd = open(notif_pipe_path, O_RDONLY)) == -1){
    fprintf(stderr, "ERROR: Error opening notifications pipe.\n");
  }

  // While the pipe is open.
  while (status != 0){
    // If notificaitons pipe passed EOF, close the pipe.
    if ((status = read(notif_fd, buffer, MAX_STRING_SIZE)) == 0){
      fprintf(stderr, "ALERT: Notifications pipe received EOF, closing.\n");
      close(notif_fd);
    }
    // If error, print that an error occured and keep going.
    else if (status == -1){
      fprintf(stderr, "ERROR: Error reading from notifications pipe.\n");
    }
    // TODO: Print the result to the client's stdout.
    else{
      fprintf(stdout, "olala.\n");
    }
  }
  return NULL;
}
