/* gridfs.c */

#include "gridfs.h"
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <math.h>
#include <fcntl.h>

#ifndef MIN
#define MIN(a,b) ( (a) < (b) ? (a) : (b) )
#endif

#ifndef MAX
#define MAX(a,b) ( (a) > (b) ? (a) : (b) )
#endif

#define DEFAULT_CHUNK_SIZE 262144

static bson_bool_t gridfs_read_chunk(gridfs_file *file, size_t n);
static void gridfs_flush_chunk(gridfs_file *file);

gridfs* gridfs_connect(mongo_connection *conn, const char *db_name) {
    const char prefix[] = "fs";  /* TODO: allow custom prefix */
    size_t dblen = strlen(db_name) + 1;
    size_t preflen = strlen(prefix) + 1;
    size_t nslen = dblen + preflen;
    char *ns;
    bson_buffer bb;
    bson b;
    bson_bool_t success = 0;
    gridfs *gridfs;

    gridfs = bson_malloc(sizeof(gridfs_file));

    ns = bson_malloc(nslen);
    strncpy(ns, db_name, dblen);
    strncat(ns, ".", 1);
    strncat(ns, prefix, preflen - 1);

    gridfs->conn = conn;

    gridfs->db_name = bson_malloc(dblen);
    strncpy(gridfs->db_name, db_name, dblen);

    gridfs->prefix = bson_malloc(preflen);
    strncpy(gridfs->prefix, prefix, preflen);

    gridfs->file_ns = bson_malloc(nslen);
    strncpy(gridfs->file_ns, ns, nslen);
    strncat(gridfs->file_ns, ".files", 6);

    gridfs->chunk_ns = bson_malloc(nslen + 7);
    strncpy(gridfs->chunk_ns, ns, nslen);
    strncat(gridfs->chunk_ns, ".chunks", 7);

    success = mongo_create_simple_index(conn, gridfs->file_ns,
                                        "filename", 0, NULL);

    bson_buffer_init(&bb);
    bson_append_int(&bb, "files_id", 1);
    bson_append_int(&bb, "n", 1);
    bson_from_buffer(&b, &bb);

    success |= mongo_create_index(conn, gridfs->chunk_ns, &b, 0, NULL);

    free(ns);
    bson_destroy(&b);

    if (!success) {
        gridfs_disconnect(gridfs);
        return NULL;
    }

    return gridfs;
}

void gridfs_disconnect(gridfs *gridfs) {
    free(gridfs->db_name);
    free(gridfs->prefix);
    free(gridfs->file_ns);
    free(gridfs->chunk_ns);
    free(gridfs);
}

gridfs_file* gridfs_open(gridfs *gridfs, const char *name, const char *mode) {
    bson_buffer bb;
    gridfs_file *file;

    if (!strcmp(mode, "r")) {
        bson b;

        bson_buffer_init(&bb);
        bson_append_string(&bb, "filename", name);
        bson_from_buffer(&b, &bb);

        file = gridfs_query(gridfs, &b);
        bson_destroy(&b);

        return file;
    } else if(!strcmp(mode, "w")) {
        file = bson_calloc(1, sizeof(gridfs_file));
        file->filename = bson_malloc(strlen(name) + 1);
        strcpy(file->filename, name);
        file->data = bson_malloc(DEFAULT_CHUNK_SIZE);
        file->chunk_size = DEFAULT_CHUNK_SIZE;
        file->mode = 'w';
        file->gridfs = gridfs;

        bson_buffer_init(&bb);
        bson_append_new_oid(&bb, "_id");
        bson_from_buffer(&file->id_b, &bb);
        bson_find(&file->id, &file->id_b, "_id");

        bson_empty(&file->metadata);

        return file;
    } else {
        return NULL;
    }
}

gridfs_file* gridfs_query(gridfs *gridfs, bson *query) {
    bson_buffer bb;
    bson_iterator it;
    bson out;
    gridfs_file *file;
    unsigned int chunk_size;
    int64_t length;
    const char *name;

    if (!mongo_find_one(gridfs->conn, gridfs->file_ns, query, NULL, &out)) {
        return NULL;
    }

    if (!bson_find(&it, &out, "chunkSize")) {
        bson_destroy(&out);
        return NULL;
    }
    chunk_size = bson_iterator_int(&it);

    if (!bson_find(&it, &out, "length")) {
        bson_destroy(&out);
        return NULL;
    }
    length = bson_iterator_long(&it);

    if (!bson_find(&it, &out, "filename")) {
        bson_destroy(&out);
        return NULL;
    }
    name = bson_iterator_string(&it);

    file = bson_calloc(1, sizeof(gridfs_file));

    file->chunk_size = chunk_size;
    file->length = length;

    file->filename = bson_malloc(strlen(name) + 1);
    strcpy(file->filename, name);

    file->data = bson_calloc(MIN(length, chunk_size), sizeof(char));
    file->gridfs = gridfs;

    if (!bson_find(&it, &out, "md5")) {
        bson_destroy(&out);
        gridfs_close(file);
        return NULL;
    }
    strncpy(file->md5, bson_iterator_string(&it), 32);

    if (!bson_find(&it, &out, "_id")) {
        bson_destroy(&out);
        gridfs_close(file);
        return NULL;
    }
    bson_buffer_init(&bb);
    bson_append_element(&bb, NULL, &it);
    bson_from_buffer(&file->id_b, &bb);
    bson_find(&file->id, &file->id_b, "_id");

    if (bson_find(&it, &out, "contentType")) {
        strncpy(file->content_type, bson_iterator_string(&it), 255);
    }

    if (bson_find(&it, &out, "uploadDate")) {
        file->upload_date = bson_iterator_time_t(&it);
    }

    if (bson_find(&it, &out, "metadata") &&
        bson_iterator_type(&it) == bson_object) {
        bson sub;
        bson_iterator_subobject(&it, &sub);
        bson_copy(&file->metadata, &sub);
    } else {
        bson_empty(&file->metadata);
    }

    bson_destroy(&out);

    if (file->length > 0 && !gridfs_read_chunk(file, 0)) {
        gridfs_close(file);
        return NULL;
    }

    file->mode = 'r';

    return file;
}

void gridfs_flush(gridfs_file *file) {
    bson_buffer bb;
    bson b, cond, out;
    bson_iterator it;
    gridfs *gridfs = file->gridfs;

    gridfs_flush_chunk(file);

    bson_buffer_init(&bb);
    bson_append_element(&bb, "filemd5", &file->id);
    bson_append_string(&bb, "root", gridfs->prefix);
    bson_from_buffer(&b, &bb);

    if (!mongo_run_command(gridfs->conn, gridfs->db_name, &b, &out)) {
        bson_destroy(&b);
        return;
    }
    bson_destroy(&b);

    if (!bson_find(&it, &out, "md5")) {
        bson_destroy(&out);
        return;
    }

    bson_buffer_init(&bb);
    bson_append_element(&bb, "_id", &file->id);
    bson_from_buffer(&cond, &bb);

    bson_buffer_init(&bb);
    bson_append_element(&bb, "_id", &file->id);
    bson_append_string(&bb, "filename", file->filename);
    bson_append_string(&bb, "contentType", file->content_type);
    bson_append_time_t(&bb, "uploadDate", file->upload_date || time(NULL));
    bson_append_long(&bb, "length", file->length);
    bson_append_long(&bb, "chunkSize", file->chunk_size);
    bson_append_string(&bb, "md5", bson_iterator_string(&it));
    bson_append_bson(&bb, "metadata", &file->metadata);
    bson_from_buffer(&b, &bb);

    mongo_update(gridfs->conn, gridfs->file_ns, &cond, &b,
                 MONGO_UPDATE_UPSERT);

    bson_destroy(&b);
    bson_destroy(&cond);
    bson_destroy(&out);
}

static void gridfs_flush_chunk(gridfs_file *file) {
    bson_buffer bb;
    bson b, cond;
    size_t size, chunk_size, length, cur_chunk;

    chunk_size = file->chunk_size;
    length = file->length;
    cur_chunk = file->cur_chunk;

    bson_buffer_init(&bb);
    bson_append_element(&bb, "files_id", &file->id);
    bson_append_long(&bb, "n", cur_chunk);
    bson_from_buffer(&cond, &bb);

    bson_buffer_init(&bb);
    bson_append_element(&bb, "files_id", &file->id);
    bson_append_long(&bb, "n", cur_chunk);

    size = MIN(chunk_size, length - (chunk_size * cur_chunk));
    bson_append_binary(&bb, "data", 0x02, file->data, size);
    bson_from_buffer(&b, &bb);

    mongo_update(file->gridfs->conn, file->gridfs->chunk_ns, &cond, &b,
                 MONGO_UPDATE_UPSERT);

    bson_destroy(&b);
    bson_destroy(&cond);
}

void gridfs_close(gridfs_file *file) {
    if (file->mode == 'w') {
        gridfs_flush(file);
    }
    bson_destroy(&file->id_b);
    bson_destroy(&file->metadata);
    free(file->filename);
    free(file->data);
    free(file);
}

size_t gridfs_read(gridfs_file *file, char *ptr, size_t size) {
    size_t pos, chunk_size, have_read, length;

    length = file->length;
    pos = file->pos;
    chunk_size = file->chunk_size;
    have_read = 0;

    while (have_read < size && pos < length) {
        size_t to_read, chunk_num = floor(pos / chunk_size);

        to_read = MIN(chunk_size - (pos % chunk_size), size - have_read);
        to_read = MIN(to_read, length - pos);

        if (chunk_num != file->cur_chunk) {
            gridfs_read_chunk(file, chunk_num);
        }

        memcpy(ptr, file->data + (pos % chunk_size), to_read);

        have_read += to_read;
        ptr += to_read;
        pos += to_read;
    }

    file->pos = pos;

    return have_read;
}

static bson_bool_t gridfs_read_chunk(gridfs_file *file, size_t n) {
    bson_buffer bb;
    bson b, out;
    bson_iterator it;
    size_t size;

    bson_buffer_init(&bb);
    bson_append_element(&bb, "files_id", &file->id);
    bson_append_long(&bb, "n", n);
    bson_from_buffer(&b, &bb);

    if (!mongo_find_one(file->gridfs->conn, file->gridfs->chunk_ns, &b,
                        NULL, &out)) {
        bson_destroy(&b);
        return 0;
    }
    bson_destroy(&b);

    if (!bson_find(&it, &out, "data")) {
        bson_destroy(&out);
        return 0;
    }

    size = MIN(file->chunk_size, bson_iterator_bin_len(&it) - 4);
    memcpy(file->data, bson_iterator_bin_data(&it) + 4, size);

    bson_destroy(&b);
    bson_destroy(&out);

    file->cur_chunk = n;

    return 1;
}

size_t gridfs_write(gridfs_file *file, const char *ptr, size_t size) {
    size_t written, pos, chunk_size;
    char *data;

    written = 0;
    pos = file->pos;
    chunk_size = file->chunk_size;
    data = file->data;

    while (written < size) {
        size_t len = MIN(chunk_size - (pos % chunk_size),
                         size - written - (pos % chunk_size));
        len = MIN(size, len);

        memcpy(data + (pos % chunk_size), ptr, len);

        pos += len;
        ptr += len;
        written += len;
        file->length = MAX(pos, file->length);

        if (!(pos % chunk_size)) {
            gridfs_flush_chunk(file);
            file->cur_chunk++;
        }
    }

    file->pos = pos;

    return written;
}

bson_bool_t gridfs_seek(gridfs_file *file, off_t offset, int origin) {
    size_t pos = file->pos, length = file->length, chunk;

    if (file->mode != 'r') {
        return 0;
    }

    if (origin == SEEK_SET) {
        pos = offset;
    } else if(origin == SEEK_CUR) {
        pos += offset;
    } else if(origin == SEEK_END) {
        pos = length - 1 - offset;
    } else {
        return 0;
    }

    if (pos > length) {
        return 0;
    }

    chunk = pos / file->chunk_size;
    if (chunk != file->cur_chunk && !gridfs_read_chunk(file, chunk)) {
            return 0;
    }

    file->pos = pos;
    return 1;
}

off_t gridfs_tell(gridfs_file *file) {
    return file->pos;
}

int gridfs_getc(gridfs_file *file) {
    char c[1];

    if(!gridfs_read(file, c, 1)) {
        return EOF;
    }

    return c[0];
}

bson_bool_t gridfs_putc(gridfs_file *file, char c) {
    return gridfs_write(file, &c, 1) == 1;
}

size_t gridfs_puts(gridfs_file *file, const char *str) {
    return gridfs_write(file, str, strlen(str));
}

char* gridfs_gets(gridfs_file *file, char *string, size_t length) {
    int c = EOF;
    char *sp = string;

    while (--length && (c = gridfs_getc(file)) != EOF) {
        *sp++ = (char)c;

        if ((char)c == '\n') {
            break;
        }
    }

    if (c == EOF && sp == string) {
        return NULL;
    }

    *sp = '\0';

    return string;
}
