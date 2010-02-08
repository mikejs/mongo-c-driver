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

size_t gridfs_read(gridfs_file file, char *ptr, size_t size);
size_t gridfs_write(gridfs_file file, const char *ptr, size_t size);

bson_bool_t gridfs_seek(gridfs_file file, off_t offset, int origin);
off_t gridfs_tell(gridfs_file file);

int gridfs_getc(gridfs_file file);
char* gridfs_gets(gridfs_file file, char *string, size_t length);

const char* gridfs_get_md5(gridfs_file file);
const char* gridfs_get_filename(gridfs_file file);
const char* gridfs_get_content_type(gridfs_file file);
time_t gridfs_get_upload_date(gridfs_file file);
size_t gridfs_get_length(gridfs_file file);
const bson* gridfs_get_metadata(gridfs_file file);

void gridfs_set_filename(gridfs_file file, const char *name);
void gridfs_set_content_type(gridfs_file file, const char *ctype);
void gridfs_set_upload_date(gridfs_file file, time_t udate);
void gridfs_set_metadata(gridfs_file file, const bson *metadata);

MONGO_EXTERN_C_END

#endif
