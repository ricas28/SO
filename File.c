/**
 * Program for creating a file struct.
 */
#include <stdio.h>
#include <string.h>
#include "File.h"

File new_file(size_t path_size, char *directory_path, char *name){
    File new_file;
    /** Give atributtes. */
    new_file.path_size = path_size;
    strcpy(new_file.directory_path, directory_path);
    strcpy(new_file.name, name);

    return new_file;
}

size_t get_path_size(File file){
    return file.path_size;
}

char *get_path_directory(File file){
    return file.directory_path;
}

char *get_name(File file){
    return file.name;
}