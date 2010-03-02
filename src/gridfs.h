/* gridfs.h */

#ifndef _GRIDFS_H_
#define _GRIDFS_H_

#include "bson.h"
#include "mongo.h"

MONGO_EXTERN_C_START

typedef struct {
    mongo_connection *conn;
    char *db_name;
    char *prefix;
    char *file_ns;
    char *chunk_ns;
} gridfs;

typedef struct {
    gridfs *gridfs;
    int64_t length, num_chunks, cur_chunk, pos;
    unsigned int chunk_size;
    char *filename, *data;
    char mode, md5[33], content_type[256];
    time_t upload_date;
    bson metadata, id_b;
    bson_iterator id;
} gridfs_file;

gridfs* gridfs_connect(mongo_connection *conn, const char *db_name);
void gridfs_disconnect(gridfs *gridfs);

gridfs_file* gridfs_open(gridfs *gridfs, const char *name, const char *mode);
gridfs_file* gridfs_query(gridfs *gridfs, bson *query);
void gridfs_close(gridfs_file *file);
void gridfs_flush(gridfs_file *file);

size_t gridfs_read(gridfs_file *file, char *ptr, size_t size);
size_t gridfs_write(gridfs_file *file, const char *ptr, size_t size);

bson_bool_t gridfs_seek(gridfs_file *file, int64_t offset, int origin);
int64_t gridfs_tell(gridfs_file *file);

int gridfs_getc(gridfs_file *file);
char* gridfs_gets(gridfs_file *file, char *string, size_t length);

bson_bool_t gridfs_putc(gridfs_file *file, char c);
size_t gridfs_puts(gridfs_file *file, const char *str);

MONGO_EXTERN_C_END

#endif
