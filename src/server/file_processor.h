#ifndef __FILE__PROCESSOR__h
#define __FILE__PROCESSOR__h

#include <dirent.h>
#include <pthread.h>

/// Creates every thread to process the .job files.
/// @param directory_path path of the folder with .job files.
/// @param MAX_BACKUPS max concurrent bakcups
/// @param MAX_THREADS max concurrent threads.
/// @param backup_mutex mutex for bakcup.
/// @param pDir DIR struct for folder with .job files.
/// @return 0 if everything was successful and 1 otherwise.
int dispatch_job_threads(char* directory_path, size_t MAX_BACKUPS, size_t MAX_THREADS, pthread_mutex_t* backup_mutex,
                      DIR* pDir);

/// Processes every command on a file.
/// @param arg pointer to arguments needed for making in and out file.
/// @return NULL
void *process_file(void *arg);

#endif