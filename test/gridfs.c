#include "mongo.h"
#include "gridfs.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#define ASSERT(x) \
    do{ \
        if(!(x)){ \
            printf("failed assert (%d): %s\n", __LINE__,  #x); \
            return 1; \
        }\
    }while(0)

#define BIG_LEN 525000

int main() {
    mongo_connection conn[1];
    gridfs gridfs;
    gridfs_file file;
    mongo_connection_options opts;
    bson b;
    bson_buffer bb;
    const bson *metadata;
    bson_iterator it;
    char data[14];
    char big[BIG_LEN], big2[BIG_LEN];
    const char *md5 = "6cd3556deb0da54bca060b4c39479839";

    strncpy(opts.host, TEST_SERVER, 255);
    opts.host[254] = '\0';
    opts.port = 27017;

    ASSERT(!mongo_connect(conn, &opts));

    if ((!mongo_cmd_drop_collection(conn, "test", "fs.chunks", NULL)
        && mongo_find_one(conn, "test.fs.chunks", bson_empty(&b),
                          bson_empty(&b), NULL)) ||
        (!mongo_cmd_drop_collection(conn, "test", "fs.files", NULL)
         && mongo_find_one(conn, "test.fs.files", bson_empty(&b),
                           bson_empty(&b), NULL)))
    {
        printf("failed to drop collections\n");
        exit(1);
    }

    ASSERT((gridfs = gridfs_connect(conn, "test")) != NULL);

    ASSERT((file = gridfs_open(gridfs, "myFile", "w")) != NULL);
    ASSERT(gridfs_write(file, "Hello, world!", 13) == 13);

    bson_buffer_init(&bb);
    bson_append_string(&bb, "str", "some metadata");
    bson_append_int(&bb, "int", 42);
    bson_from_buffer(&b, &bb);
    gridfs_set_metadata(file, &b);
    bson_destroy(&b);

    gridfs_set_content_type(file, "text/plain");

    gridfs_close(file);

    ASSERT((file = gridfs_open(gridfs, "myFile", "r")) != NULL);
    ASSERT(strcmp(md5, gridfs_get_md5(file)) == 0);

    ASSERT(gridfs_read(file, data, 13) == 13);
    data[13] = '\0';
    ASSERT(strcmp(data, "Hello, world!") == 0);

    ASSERT(gridfs_seek(file, 2, SEEK_SET));
    ASSERT(gridfs_tell(file) == 2);

    ASSERT(gridfs_read(file, data, 11) == 11);
    data[11] = '\0';
    ASSERT(strcmp(data, "llo, world!") == 0);

    metadata = gridfs_get_metadata(file);
    ASSERT(bson_find(&it, metadata, "str"));
    ASSERT(strcmp(bson_iterator_string(&it), "some metadata") == 0);
    ASSERT(bson_find(&it, metadata, "int"));
    ASSERT(bson_iterator_int(&it) == 42);

    ASSERT(strcmp(gridfs_get_content_type(file), "text/plain") == 0);

    gridfs_close(file);

    ASSERT((file = gridfs_open(gridfs, "myFile2", "w")) != NULL);
    ASSERT(gridfs_write(file, "Line 1\nLine 2\nLine 3", 21) == 21);
    gridfs_close(file);

    ASSERT((file = gridfs_open(gridfs, "myFile2", "r")) != NULL);
    ASSERT(gridfs_gets(file, data, 13) != NULL);
    ASSERT(strcmp(data, "Line 1\n") == 0);

    ASSERT(gridfs_gets(file, data, 13) != NULL);
    ASSERT(strcmp(data, "Line 2\n") == 0);

    ASSERT(gridfs_gets(file, data, 13) != NULL);
    ASSERT(strcmp(data, "Line 3") == 0);

    ASSERT(gridfs_gets(file, data, 13) == NULL);

    gridfs_close(file);

    memset(big, 'a', BIG_LEN);
    ASSERT((file = gridfs_open(gridfs, "bigFile", "w")) != NULL);
    ASSERT(gridfs_write(file, big, BIG_LEN) == BIG_LEN);
    gridfs_close(file);

    memset(big2, 0, BIG_LEN);
    ASSERT((file = gridfs_open(gridfs, "bigFile", "r")) != NULL);
    ASSERT(gridfs_get_length(file) == BIG_LEN);
    ASSERT(gridfs_read(file, big2, BIG_LEN) == BIG_LEN);
    ASSERT(memcmp(big, big2, BIG_LEN) == 0);

    gridfs_close(file);

    gridfs_disconnect(gridfs);

    return 0;
}
