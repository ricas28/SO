#include <stdio.h> 

/** Struct that represents a file. */
typedef struct File{
  size_t path_size;
  char *directory_path;
  char *name;
}File;

File new_file(size_t path_size, char *directory_path, char *name);
size_t get_path_size(File file);
char *get_path_directory(File file);
char *get_name(File file);