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
  int read_fd, write_fd; 
  int max_backups = atoi(argv[2]);
  int backups_left = max_backups;
  DIR* pDir;

  if (kvs_init()) {
    fprintf(stderr, "Failed to initialize KVS\n");
    return -1;
  }

  if(argc < 4){
    fprintf(stderr, "Insufficient number of arguments. Use: %s<directory path><number of backups>\n", argv[0]);
    return -1;
  }
  size_t directory_size = strlen(argv[1]);
  
  if((pDir = opendir(argv[1])) == NULL){
    fprintf(stderr, "Failed to open directory\n");
    return -1;
  }

  struct dirent* file_dir;
  /** Keep running until there's no files to read. */
  while ((file_dir = readdir(pDir)) != NULL) {
    char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    unsigned int delay;
    size_t num_pairs;
     
    size_t file_name_size = strlen(file_dir->d_name);
    /** Check if file is good to open (needs to be an actual .job file). */
    if(!strcmp(file_dir->d_name, ".") || !strcmp(file_dir->d_name, "..") ||  
          !(file_dir->d_name[file_name_size-1] == 'b' && file_dir->d_name[file_name_size-2] == 'o' &&
          file_dir->d_name[file_name_size-3] == 'j' && file_dir->d_name[file_name_size-4] == '.'))
        continue;
      
    /** Build relative path of file. */
    char file_directory[directory_size + file_name_size + 1];
    snprintf(file_directory, sizeof(file_directory), "%s%s", argv[1], file_dir->d_name);

    if ((read_fd = open(file_directory, O_RDONLY)) == -1) {
      fprintf(stderr, "Error opening read file: %s\n", file_directory);
      continue;
    }

    /** Build relative path for the output file. */
    size_t length = strlen(file_directory);
    char write_directory[length+1]; 
    strcpy(write_directory, file_directory);
    write_directory[length-1] = 't';
    write_directory[length-2] = 'u';
    write_directory[length-3] = 'o';

    /** Open output file. */
    write_fd = open(write_directory, O_CREAT | O_TRUNC | O_WRONLY, S_IRUSR | S_IWUSR);
    if(write_fd == -1){
      fprintf(stderr, "Error opening output file\n");
      close(read_fd);
      continue;
    }

    int quit = 0;
    while(!quit)
      switch (get_next(read_fd)) {
        case CMD_WRITE:
          num_pairs = parse_write(read_fd, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
          if (num_pairs == 0) {
            fprintf(stderr, "Invalid command. See HELP for usage\n");
            continue;
          }

          if (kvs_write(num_pairs, keys, values)) {
            fprintf(stderr, "Failed to write pair\n");
          }

          break;

        case CMD_READ:
          num_pairs = parse_read_delete(read_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

          if (num_pairs == 0) {
            fprintf(stderr, "Invalid command. See HELP for usage\n");
            continue;
          }

          if (kvs_read(num_pairs, keys, write_fd)) {
            fprintf(stderr, "Failed to read pair\n");
          }
          break;

        case CMD_DELETE:
          num_pairs = parse_read_delete(read_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

          if (num_pairs == 0) {
            fprintf(stderr, "Invalid command. See HELP for usage\n");
            continue;
          }

          if (kvs_delete(num_pairs, keys, write_fd)) {
            fprintf(stderr,"Failed to delete pair\n");
          }
          break;

        case CMD_SHOW:
          kvs_show(write_fd);
          break;

        case CMD_WAIT:
          if (parse_wait(read_fd, &delay, NULL) == -1) {
            fprintf(stderr, "Failed to read pair\n");
            continue;
          }

          if (delay > 0) {
            char message[] = "Waiting...\n";
            write(write_fd, message, sizeof(message) - 1);
            kvs_wait(delay);
          }
          break;

        case CMD_BACKUP:
          if (kvs_backup(file_directory, &backups_left, max_backups)) { //args: file directory, backup number
            fprintf(stderr,"Failed to perform backup.\n");
          }
          break;

        case CMD_INVALID:
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          break;

        case CMD_HELP:
          char buffer[] = 
              "Available commands:\n"
              "  WRITE [(key,value)(key2,value2),...]\n"
              "  READ [key,key2,...]\n"
              "  DELETE [key,key2,...]\n"
              "  SHOW\n"
              "  WAIT <delay_ms>\n"
              "  BACKUP\n" // Not implemented
              "  HELP\n"
          ;
          write_buffer(write_fd, buffer, strlen(buffer));

          break;
          
        case CMD_EMPTY:
          break;
        case EOC:
          /** Close input and output file*/
          close(read_fd);
          close(write_fd);
          quit = 1;
    }
  }
  /** Ending program. */
  kvs_terminate();
  closedir(pDir);
  return 0;
}
