/* Counts the rows of a Parquet file through the C stream reader: downstream-c FILE */
#include <stdio.h>
#include <string.h>

#include <tparquet_read.h>

int main(int argc, char **argv) {
    struct ArrowArrayStream s;
    char msg[512];
    long long rows = 0;
    if (argc != 2) return 2;
    if (tpr_open(argv[1], NULL, 0, 0, &s) != TPR_OK) {
        tpr_last_error(msg, sizeof msg);
        fprintf(stderr, "%s\n", msg);
        return 1;
    }
    for (;;) {
        struct ArrowArray a;
        if (s.get_next(&s, &a) != 0) {
            fprintf(stderr, "%s\n", s.get_last_error(&s));
            s.release(&s);
            return 1;
        }
        if (a.release == NULL) break;
        rows += a.length;
        a.release(&a);
    }
    s.release(&s);
    printf("%lld rows\n", rows);
    return 0;
}
