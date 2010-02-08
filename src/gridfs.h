/* gridfs.h */

#ifndef _GRIDFS_H_
#define _GRIDFS_H_

#include "bson.h"
#include "mongo.h"

MONGO_EXTERN_C_START

typedef struct gridfs_t *gridfs;

typedef struct gridfs_file_t *gridfs_file;

gridfs gridfs_connect(mongo_connection *conn, const char *db_name);
void gridfs_disconnect(gridfs gridfs);

gridfs_file gridfs_open(gridfs gridfs, const char *name, const char *mode);
void gridfs_close(gridfs_file file);
void gridfs_flush(gridfs_file file);

size_t gridfs_read(char *ptr, size_t size, gridfs_file file);
size_t gridfs_write(const char *ptr, size_t size, gridfs_file file);

bson_bool_t gridfs_seek(gridfs_file file, off_t offset, int origin);
off_t gridfs_tell(gridfs_file file);

int gridfs_getc(gridfs_file file);
char* gridfs_gets(char *string, size_t length, gridfs_file file);

const char* gridfs_get_md5(gridfs_file file);
const char* gridfs_get_filename(gridfs_file file);
const char* gridfs_get_content_type(gridfs_file file);
time_t gridfs_get_upload_date(gridfs_file file);
size_t gridfs_get_length(gridfs_file file);

void gridfs_set_filename(const char *name, gridfs_file file);
void gridfs_set_content_type(const char *ctype, gridfs_file file);
void gridfs_set_upload_date(time_t udate, gridfs_file file);

MONGO_EXTERN_C_END

#endif
