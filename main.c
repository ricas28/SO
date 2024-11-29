#include <limits.h>
#include <fcntl.h>
#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

#include "constants.h"
#include "parser.h"
#include "operations.h"

// check if the bytes were written in the file, not necessary, blame OS

int main(int argc, char** argv) {
  
  int write_fd; 
  DIR* pDir = opendir(argv[1]);

  if (kvs_init()) {
    fprintf(stderr, "Failed to initialize KVS\n");
    return 1;
  }

  if(argc < 4){
    fprintf(stderr, "Insufficient number of arguments. Use: %s<directory path><number of backups>\n", argv[0]);
  }
  


  while (1) {
    char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    unsigned int delay;
    size_t num_pairs;
    char* unknownCommands = "Invalid command. See HELP for usage\n";

    struct dirent* file_dir = readdir(pDir);
    if(file_dir == NULL){
      fprintf(stderr, "Error opening directory.\n");
      return -1;
    }

    char* file_directory = strcat(argv[1], file_dir->d_name);

    int read_fd = open(file_directory, O_RDONLY);
    if (read_fd == -1){
      fprintf(stderr, "Error opening %s\n", file_directory);
    }

    char* write_directory = strcpy(write_directory, file_directory);
    int length = strlen(file_directory);
    file_directory[length-1] = 't';
    file_directory[length-2] = 'u';
    file_directory[length-3] = 'o';


    int write_fd = open(write_directory, O_CREAT || O_TRUNC);

    switch (get_next(read_fd)) {
      case CMD_WRITE:
        num_pairs = parse_write(read_fd, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
        if (num_pairs == 0) {
          write(write_fd, (void*)unknownCommands, strlen(unknownCommands));
          continue;
        }

        if (kvs_write(num_pairs, keys, values)) {
          char* errorMessage = "Failed to write pair\n";
          write(write_fd, (void*)errorMessage, strlen(errorMessage));
        }

        break;

      case CMD_READ:
        num_pairs = parse_read_delete(read_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

        if (num_pairs == 0) {
          write(write_fd, (void*)unknownCommands, strlen(unknownCommands));
          continue;
        }

        if (kvs_read(num_pairs, keys)) {
          char* errorMessage = "Failed to read pair\n";
          write(write_fd, (void*)errorMessage, strlen(errorMessage));
        }
        break;

      case CMD_DELETE:
        num_pairs = parse_read_delete(read_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

        if (num_pairs == 0) {
          write(write_fd, (void*)unknownCommands, strlen(unknownCommands));
          continue;
        }

        if (kvs_delete(num_pairs, keys)) {
          char* errorMessage = "Failed to delete pair\n";
          write(write_fd, (void*)errorMessage, strlen(errorMessage));
        }
        break;

      case CMD_SHOW:

        kvs_show();
        break;

      case CMD_WAIT:
        if (parse_wait(read_fd, &delay, NULL) == -1) {
          write(write_fd, (void*)unknownCommands, strlen(unknownCommands));
          continue;
        }

        if (delay > 0) {
          char* message = "Waiting...\n";
          write(write_fd, (void*)message, strlen(message)-1);
          kvs_wait(delay);
        }
        break;

      case CMD_BACKUP:

        if (kvs_backup()) {
          char* errorMessage = "Failed to perform backup.\n";
          write(write_fd, (void*)errorMessage, strlen(errorMessage));
        }
        break;

      case CMD_INVALID:
        write(write_fd, (void*)unknownCommands, strlen(unknownCommands));
        break;

      case CMD_HELP:
        printf( 
            "Available commands:\n"
            "  WRITE [(key,value)(key2,value2),...]\n"
            "  READ [key,key2,...]\n"
            "  DELETE [key,key2,...]\n"
            "  SHOW\n"
            "  WAIT <delay_ms>\n"
            "  BACKUP\n" // Not implemented
            "  HELP\n"
        );

        break;
        
      case CMD_EMPTY:
        break;

      case EOC:
        kvs_terminate();
        return 0;
    }
  }
}
