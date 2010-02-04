/* gridfs.h */

#ifndef _GRIDFS_H_
#define _GRIDFS_H_

#include "bson.h"
#include "mongo.h"

MONGO_EXTERN_C_START

#define DEFAULT_CHUNK_SIZE 262144

typedef struct {
    mongo_connection *conn;
    char *db_name;
    char *prefix;
    char *file_ns;
    char *chunk_ns;
} gridfs;

typedef struct {
    size_t length, chunk_size, num_chunks, cur_chunk, pos;
    char *filename;
    char md5[33];
    char content_type[256];
    bson_oid_t id;
    time_t upload_date;
    bson b;
    char *data;
    gridfs *gridfs;
    char mode;
} gridfs_file;

bson_bool_t gridfs_connect(gridfs *gridfs, mongo_connection *conn,
                           const char *db_name);

gridfs_file* gridfs_open(gridfs *gridfs, const char *name, const char *mode);

size_t gridfs_read(char *ptr, size_t size, gridfs_file *file);

size_t gridfs_write(const char *ptr, size_t size, gridfs_file *file);

int gridfs_seek(gridfs_file *file, long offset, int whence);

void gridfs_flush(gridfs_file *file);

void gridfs_close(gridfs_file *file);

MONGO_EXTERN_C_END

#endif
