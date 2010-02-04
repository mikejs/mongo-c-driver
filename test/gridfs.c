#include "mongo.h"
#include "gridfs.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

int main() {
    mongo_connection conn[1];
    gridfs gridfs[1];
    gridfs_file *file;
    mongo_connection_options opts;
    bson b;
    char data[14];
    const char *md5 = "6cd3556deb0da54bca060b4c39479839";

    strncpy(opts.host, TEST_SERVER, 255);
    opts.host[254] = '\0';
    opts.port = 27017;

    if (mongo_connect(conn, &opts)) {
        printf("failed to connect\n");
        exit(1);
    }

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

    if (!gridfs_connect(gridfs, conn, "test")) {
        printf("failed gridfs creation\n");
        exit(1);
    }

    file = gridfs_open(gridfs, "myFile", "w");
    if (file == NULL) {
        printf("failed opening myFile for writing\n");
        exit(1);
    }

    if (gridfs_write("Hello, world!", 13, file) != 13) {
        printf("failed gridfs_write\n");
        exit(1);
    }

    gridfs_close(file);

    file = gridfs_open(gridfs, "myFile", "r");
    if (file == NULL) {
        printf("failed opening myFile for reading\n");
        exit(1);
    }

    if (strcmp(md5, file->md5)) {
        printf("md5 doesn't match\n");
        exit(1);
    }

    if (gridfs_read(data, 13, file) != 13) {
        printf("failed read\n");
        exit(1);
    }
    data[13] = '\0';

    if (strcmp(data, "Hello, world!")) {
        printf("read bad data\n");
        exit(1);
    }

    return 0;
}
