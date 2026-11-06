/* Links libtparquet_read from plain C and reads a PCODEC file as an Arrow C stream:
 * `make -C rust/cread c-test TESTDATA=...`. Exits non-zero (with a message) on the first
 * failed check. */
#include "tparquet_read.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int failures = 0;

#define CHECK(cond)                                                     \
    do {                                                                \
        if (!(cond)) {                                                  \
            fprintf(stderr, "smoke.c:%d: check failed: %s\n", __LINE__, #cond); \
            failures++;                                                 \
        }                                                               \
    } while (0)

static void path_of(char *buf, size_t cap, const char *dir, const char *name) {
    snprintf(buf, cap, "%s/files/%s", dir, name);
}

/* every row of the stream, or -1 on a get_next error */
static int64_t count_rows(struct ArrowArrayStream *s, int64_t max_batch) {
    int64_t rows = 0;
    for (;;) {
        struct ArrowArray a;
        memset(&a, 0, sizeof a);
        if (s->get_next(s, &a) != 0) return -1;
        if (a.release == NULL) return rows;
        if (a.length > max_batch) return -2;
        if (a.n_children < 1 && rows == 0) return -3;
        rows += a.length;
        a.release(&a);
    }
}

int main(int argc, char **argv) {
    const char *dir = argc > 1 ? argv[1] : "testdata";
    char path[4096], msg[512];
    CHECK(tpr_abi_version() == TPR_ABI_VERSION);

    /* the whole file */
    path_of(path, sizeof path, dir, "mixed.parquet");
    struct ArrowArrayStream s;
    memset(&s, 0, sizeof s);
    CHECK(tpr_open(path, NULL, 0, 500, &s) == TPR_OK);
    CHECK(s.release != NULL);
    struct ArrowSchema schema;
    CHECK(s.get_schema(&s, &schema) == 0);
    CHECK(strcmp(schema.format, "+s") == 0);
    CHECK(schema.n_children == 7);
    CHECK(strcmp(schema.children[0]->name, "ts") == 0);
    CHECK(strncmp(schema.children[0]->format, "ts", 2) == 0); /* a timestamp */
    CHECK(strcmp(schema.children[1]->format, "g") == 0); /* price: double */
    schema.release(&schema);
    CHECK(count_rows(&s, 500) == 1500);
    CHECK(s.get_last_error(&s) == NULL);
    s.release(&s);
    CHECK(s.release == NULL);

    /* a projection, in the requested order */
    const char *cols[] = {"price", "ts"};
    CHECK(tpr_open(path, cols, 2, 0, &s) == TPR_OK);
    CHECK(s.get_schema(&s, &schema) == 0);
    CHECK(schema.n_children == 2);
    CHECK(strcmp(schema.children[0]->name, "price") == 0);
    CHECK(strcmp(schema.children[1]->name, "ts") == 0);
    schema.release(&schema);
    CHECK(count_rows(&s, TPR_DEFAULT_BATCH_SIZE) == 1500);
    s.release(&s);

    /* footer helpers */
    int64_t rows = -1, groups = -1;
    path_of(path, sizeof path, dir, "multiple_row_groups.parquet");
    CHECK(tpr_file_info(path, &rows, &groups) == TPR_OK);
    CHECK(rows == 2000 && groups == 5);
    CHECK(tpr_file_schema(path, &schema) == TPR_OK);
    CHECK(strcmp(schema.format, "+s") == 0);
    schema.release(&schema);
    CHECK(tpr_last_error(msg, sizeof msg) == 0);

    /* errors: a missing file, an unknown column, NULL arguments */
    CHECK(tpr_open("/nonexistent/x.parquet", NULL, 0, 0, &s) == TPR_ERR_IO);
    CHECK(tpr_last_error(msg, sizeof msg) > 0 && strstr(msg, "x.parquet") != NULL);
    const char *bad[] = {"nope"};
    path_of(path, sizeof path, dir, "mixed.parquet");
    CHECK(tpr_open(path, bad, 1, 0, &s) == TPR_ERR_ARGUMENT);
    tpr_last_error(msg, sizeof msg);
    CHECK(strcmp(msg, "no column named 'nope'") == 0);
    CHECK(tpr_open(NULL, NULL, 0, 0, &s) == TPR_ERR_ARGUMENT);
    CHECK(tpr_open(path, NULL, 0, 0, NULL) == TPR_ERR_ARGUMENT);

    /* a stream callback error: EINVAL and a message */
    CHECK(tpr_open(path, NULL, 0, 0, &s) == TPR_OK);
    CHECK(s.get_next(&s, NULL) == EINVAL);
    CHECK(s.get_last_error(&s) != NULL && strcmp(s.get_last_error(&s), "NULL `out`") == 0);
    s.release(&s);

    if (failures) {
        fprintf(stderr, "smoke.c: %d checks failed\n", failures);
        return 1;
    }
    printf("ok: tparquet_read C smoke test\n");
    return 0;
}
