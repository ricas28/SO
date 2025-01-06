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

int kvs_connect(char const *req_pipe_path, char const *resp_pipe_path,
                char const *server_pipe_path, char const *notif_pipe_path
                ) {
  int error = 0;
  size_t req_pipe_size;
  size_t resp_pipe_size;
  size_t notif_pipe_size;

  /* Create FIFOs. */
  if (mkfifo(req_pipe_path, 0666) != 0){fprintf(stderr, "ERROR: Creating request pipe.\n"); error = 1;}
  if (mkfifo(resp_pipe_path, 0666) != 0){fprintf(stderr, "ERROR: Creating response pipe.\n"); error = 1;}
  if (mkfifo(notif_pipe_path, 0666) != 0){fprintf(stderr, "ERROR: Creating notifications pipe.\n"); error = 1;}

  if (error != 0){return error;} // Not worth it going further.

  /* Send the message through the server pipe. */
  req_pipe_size = strlen(req_pipe_path);
  resp_pipe_size = strlen(resp_pipe_path);
  notif_pipe_size = strlen(notif_pipe_path);

  char buffer[MAX_PIPE_PATH_LENGTH*3 + 1]; //buffer to store the path names and the opcode 1.
  buffer[0] = '1'; //OPCODE for kvs_connect.

  /* Concatenate the pipe names to the buffer. 
  *  And fill the rest of the buffer with '\0'.
  */
  strncat(buffer, req_pipe_path, req_pipe_size);
  for (size_t i = req_pipe_size + 1; i <= MAX_PIPE_PATH_LENGTH + 1; i++){
    buffer[i] = '\0';
  }
  strncat(buffer, resp_pipe_path, resp_pipe_size);
  for (size_t i = resp_pipe_size + MAX_PIPE_PATH_LENGTH + 1; i <= MAX_PIPE_PATH_LENGTH*2 + 1; i++){
    buffer[i] = '\0';
  }
  strncat(buffer, notif_pipe_path, notif_pipe_size);
  for (size_t i = notif_pipe_size + MAX_PIPE_PATH_LENGTH*2 + 1; i <= MAX_PIPE_PATH_LENGTH*3 + 1; i++){
    buffer[i] = '\0';
  }

  /** Open server pipe and send the message. */
  int server_fd = open(server_pipe_path, O_WRONLY);
  write_all(server_fd, buffer, MAX_PIPE_PATH_LENGTH*3 + 1);
  return error;
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
