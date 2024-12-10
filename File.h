#include <stdio.h> 

#include "constants.h"

/** Struct that represents a file. */
typedef struct File{
  size_t path_size;
  char directory_path[MAX_JOB_FILE_NAME_SIZE];
  char name[MAX_JOB_FILE_NAME_SIZE];
}File;

File *new_file(size_t path_size, char *directory_path, char *name);
size_t get_path_size(File *file);
char *get_file_directory(File *file);
char *get_file_name(File *file);