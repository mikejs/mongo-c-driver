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

struct gridfs_t {
    mongo_connection *conn;
    char *db_name;
    char *prefix;
    char *file_ns;
    char *chunk_ns;
};

struct gridfs_file_t {
    size_t length, chunk_size, num_chunks, cur_chunk;
    off_t pos;
    char *filename;
    char md5[33];
    char content_type[256];
    bson_oid_t id;
    time_t upload_date;
    bson b;
    char *data;
    gridfs gridfs;
    char mode;
};

gridfs_file gridfs_open_readonly(gridfs gridfs, const char *name);
bson_bool_t gridfs_read_chunk(gridfs_file file, size_t n);
void gridfs_flush_chunk(gridfs_file file);

gridfs gridfs_connect(mongo_connection *conn, const char *db_name) {
    const char prefix[] = "fs";  /* TODO: allow custom prefix */
    size_t dblen = strlen(db_name) + 1;
    size_t preflen = strlen(prefix) + 1;
    size_t nslen = dblen + preflen;
    char *ns;
    bson_buffer bb;
    bson b;
    bson_bool_t success = 0;
    gridfs gridfs;

    gridfs = malloc(sizeof(struct gridfs_t));

    ns = malloc(nslen);
    strncpy(ns, db_name, dblen);
    strncat(ns, ".", 1);
    strncat(ns, prefix, preflen - 1);

    gridfs->conn = conn;

    gridfs->db_name = malloc(dblen);
    strncpy(gridfs->db_name, db_name, dblen);

    gridfs->prefix = malloc(preflen);
    strncpy(gridfs->prefix, prefix, preflen);

    gridfs->file_ns = malloc(nslen);
    strncpy(gridfs->file_ns, ns, nslen);
    strncat(gridfs->file_ns, ".files", 6);

    gridfs->chunk_ns = malloc(nslen + 7);
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

void gridfs_disconnect(gridfs gridfs) {
    free(gridfs->db_name);
    free(gridfs->prefix);
    free(gridfs->file_ns);
    free(gridfs->chunk_ns);
    free(gridfs);
}

gridfs_file gridfs_open(gridfs gridfs, const char *name, const char *mode) {
    gridfs_file f;
    char *filename, *data;

    if (!strcmp(mode, "r")) {
        return gridfs_open_readonly(gridfs, name);
    } else if(!strcmp(mode, "w")) {
        f = calloc(1, sizeof(struct gridfs_file_t));
        filename = malloc(strlen(name) + 1);
        data = malloc(DEFAULT_CHUNK_SIZE);
        if (f == NULL || filename == NULL || data == NULL) {
            free(f);
            free(filename);
            free(data);
            return NULL;
        }

        strcpy(filename, name);
        f->filename = filename;
        f->data = data;
        f->chunk_size = DEFAULT_CHUNK_SIZE;
        f->mode = 'w';
        f->gridfs = gridfs;
        bson_oid_gen(&f->id);

        return f;
    } else {
        return NULL;
    }
}

size_t gridfs_write(const char *ptr, size_t size, gridfs_file file) {
    size_t written, pos, chunk_size, length;
    char *data;

    written = 0;
    pos = file->pos;
    chunk_size = file->chunk_size;
    data = file->data;
    length = file->length;

    while (written < size) {
        size_t len = MIN(chunk_size, size - written - (pos % chunk_size));
        memcpy(data, ptr, len);

        pos += len;
        length = MAX(pos, length);

        if (!(pos % chunk_size)) {
            gridfs_flush_chunk(file);
            file->cur_chunk++;
        }

        ptr += len;
        written += len;
    }

    file->length = length;
    file->pos = pos;

    return written;
}

void gridfs_flush(gridfs_file file) {
    bson_buffer bb;
    bson b, cond, out;
    bson_iterator it;
    gridfs gridfs = file->gridfs;

    gridfs_flush_chunk(file);

    bson_buffer_init(&bb);
    bson_append_oid(&bb, "filemd5", &file->id);
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
    bson_append_oid(&bb, "_id", &file->id);
    bson_from_buffer(&cond, &bb);

    bson_buffer_init(&bb);
    bson_append_oid(&bb, "_id", &file->id);
    bson_append_string(&bb, "filename", file->filename);
    bson_append_string(&bb, "contentType", file->content_type);
    bson_append_time_t(&bb, "uploadDate", file->upload_date || time(NULL));
    bson_append_long(&bb, "length", file->length);
    bson_append_long(&bb, "chunkSize", file->chunk_size);
    bson_append_string(&bb, "md5", bson_iterator_string(&it));
    bson_from_buffer(&b, &bb);

    mongo_update(gridfs->conn, gridfs->file_ns, &cond, &b,
                 MONGO_UPDATE_UPSERT);

    bson_destroy(&b);
    bson_destroy(&cond);
    bson_destroy(&out);
}

void gridfs_flush_chunk(gridfs_file file) {
    bson_buffer bb;
    bson b, cond;
    size_t size, chunk_size, length, cur_chunk;

    chunk_size = file->chunk_size;
    length = file->length;
    cur_chunk = file->cur_chunk;

    bson_buffer_init(&bb);
    bson_append_oid(&bb, "files_id", &file->id);
    bson_append_long(&bb, "n", cur_chunk);
    bson_from_buffer(&cond, &bb);

    bson_buffer_init(&bb);
    bson_append_oid(&bb, "files_id", &file->id);
    bson_append_long(&bb, "n", cur_chunk);

    size = MIN(chunk_size, length - (chunk_size * cur_chunk));

    bson_append_binary(&bb, "data", 0x02, file->data, size);
    bson_from_buffer(&b, &bb);

    mongo_update(file->gridfs->conn, file->gridfs->chunk_ns, &cond, &b,
                 MONGO_UPDATE_UPSERT);

    bson_destroy(&b);
    bson_destroy(&cond);
}

size_t gridfs_read(char *ptr, size_t size, gridfs_file file) {
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

bson_bool_t gridfs_read_chunk(gridfs_file file, size_t n) {
    bson_buffer bb;
    bson b, out;
    bson_iterator it;
    size_t size;

    bson_buffer_init(&bb);
    bson_append_oid(&bb, "files_id", &file->id);
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

gridfs_file gridfs_open_readonly(gridfs gridfs, const char *name) {
    bson_buffer bb;
    bson b, out;
    bson_iterator it;
    gridfs_file file;
    size_t chunk_size, length, flen;
    char *data, *filename;

    bson_buffer_init(&bb);
    bson_append_string(&bb, "filename", name);
    bson_from_buffer(&b, &bb);

    if (!mongo_find_one(gridfs->conn, gridfs->file_ns, &b, NULL, &out)) {
        bson_destroy(&b);
        return NULL;
    }
    bson_destroy(&b);

    if (!bson_find(&it, &out, "chunkSize")) {
        bson_destroy(&out);
        return NULL;
    }
    chunk_size = bson_iterator_long(&it);

    if (!bson_find(&it, &out, "length")) {
        bson_destroy(&out);
        return NULL;
    }
    length = bson_iterator_long(&it);

    file = calloc(1, sizeof(struct gridfs_file_t));
    data = calloc(MIN(length, chunk_size), sizeof(char));
    flen = strlen(name) + 1;
    filename = malloc(flen);
    if (file == NULL || data == NULL || filename == NULL) {
        bson_destroy(&out);
        free(file);
        free(data);
        free(filename);
        return NULL;
    }

    file->chunk_size = chunk_size;
    file->length = length;
    strncpy(filename, name, flen);
    file->filename = filename;
    file->data = data;
    file->gridfs = gridfs;

    if (!bson_find(&it, &out, "md5")) {
        bson_destroy(&out);
        gridfs_close(file);
        return NULL;
    }
    strncpy(file->md5, bson_iterator_string(&it), 32);

    if (!bson_find(&it, &out, "_id") ||
        bson_iterator_type(&it) != bson_oid) {
        bson_destroy(&b);
        gridfs_close(file);
        return NULL;
    }
    memcpy(&file->id, bson_iterator_oid(&it), sizeof(bson_oid_t));

    if (bson_find(&it, &out, "contentType")) {
        strncpy(file->content_type, bson_iterator_string(&it), 255);
    }

    if (bson_find(&it, &out, "uploadDate")) {
        file->upload_date = bson_iterator_time_t(&it);
    }

    bson_destroy(&out);

    if (file->length > 0 && !gridfs_read_chunk(file, 0)) {
        gridfs_close(file);
        return NULL;
    }

    file->mode = 'r';

    return file;
}

bson_bool_t gridfs_seek(gridfs_file file, off_t offset, int origin) {
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

off_t gridfs_tell(gridfs_file file) {
    return file->pos;
}

int gridfs_getc(gridfs_file file) {
    char c[1];

    if(!gridfs_read(c, 1, file)) {
        return EOF;
    }

    return c[0];
}

char* gridfs_gets(char *string, size_t length, gridfs_file file) {
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

void gridfs_close(gridfs_file file) {
    if (file->mode == 'w') {
        gridfs_flush(file);
    }
    bson_destroy(&file->b);
    free(file->filename);
    free(file->data);
    free(file);
}

const char* gridfs_get_md5(gridfs_file file) {
    return file->md5;
}

const char* gridfs_get_filename(gridfs_file file) {
    return file->filename;
}

const char* gridfs_get_content_type(gridfs_file file) {
    return file->content_type;
}

time_t gridfs_get_upload_date(gridfs_file file) {
    time_t udate = file->upload_date;

    if (udate == 0) {
        return -1;
    }

    return udate;
}

void gridfs_set_filename(const char *name, gridfs_file file) {
    size_t flen = strlen(name) + 1;
    free(file->filename);
    file->filename = malloc(flen);
    strncpy(file->filename, name, flen);
}

void gridfs_set_content_type(const char *ctype, gridfs_file file) {
    strncpy(file->content_type, ctype, 255);
}

void gridfs_set_upload_date(time_t udate, gridfs_file file) {
    file->upload_date = udate;
}
