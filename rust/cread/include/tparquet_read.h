/* C reading API for Parquet files, PCODEC (tparquet spec/FORMAT.md) or not, as Arrow C streams
 * (https://arrow.apache.org/docs/format/CStreamInterface.html). Link libtparquet_read.a (with
 * -lpthread -ldl -lm) or libtparquet_read.so.
 *
 * Thread-safe: every call is independent, and a stream may be used from any one thread at a
 * time. Pointers may be NULL only where stated. Errors are a return code plus a message:
 * tpr_last_error() for the functions below (per thread), the stream's get_last_error() for
 * its callbacks. No Rust panic crosses this interface: an internal error is TPR_ERR_PANIC
 * (or EIO from a stream callback) with a message. */
#ifndef TPARQUET_READ_H
#define TPARQUET_READ_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* The Arrow C data and C stream interfaces, as the Arrow specification defines them (the
 * guards let this header coexist with Arrow's own abi.h and other copies). */
#ifndef ARROW_C_DATA_INTERFACE
#define ARROW_C_DATA_INTERFACE

#define ARROW_FLAG_DICTIONARY_ORDERED 1
#define ARROW_FLAG_NULLABLE 2
#define ARROW_FLAG_MAP_KEYS_SORTED 4

struct ArrowSchema {
    const char *format;
    const char *name;
    const char *metadata;
    int64_t flags;
    int64_t n_children;
    struct ArrowSchema **children;
    struct ArrowSchema *dictionary;
    void (*release)(struct ArrowSchema *);
    void *private_data;
};

struct ArrowArray {
    int64_t length;
    int64_t null_count;
    int64_t offset;
    int64_t n_buffers;
    int64_t n_children;
    const void **buffers;
    struct ArrowArray **children;
    struct ArrowArray *dictionary;
    void (*release)(struct ArrowArray *);
    void *private_data;
};

#endif /* ARROW_C_DATA_INTERFACE */

#ifndef ARROW_C_STREAM_INTERFACE
#define ARROW_C_STREAM_INTERFACE

struct ArrowArrayStream {
    int (*get_schema)(struct ArrowArrayStream *, struct ArrowSchema *out);
    int (*get_next)(struct ArrowArrayStream *, struct ArrowArray *out);
    const char *(*get_last_error)(struct ArrowArrayStream *);
    void (*release)(struct ArrowArrayStream *);
    void *private_data;
};

#endif /* ARROW_C_STREAM_INTERFACE */

#define TPR_ABI_VERSION 1

/* return codes of the tpr_ functions (stream callbacks return errno values: 0, EINVAL, EIO) */
#define TPR_OK 0
#define TPR_ERR_ARGUMENT 1 /* bad argument: NULL pointer, unknown or repeated column */
#define TPR_ERR_IO 2       /* the file cannot be opened or read */
#define TPR_ERR_PARQUET 3  /* not a Parquet file, or one this reader cannot read */
#define TPR_ERR_PANIC 4    /* internal error; please report it */

/* rows per batch when batch_size is 0 */
#define TPR_DEFAULT_BATCH_SIZE 8192

/* TPR_ABI_VERSION of the linked library */
uint32_t tpr_abi_version(void);

/* Opens the Parquet file at path (NUL-terminated, UTF-8 on Windows, bytes elsewhere) and moves
 * a stream of its rows into *out, which the caller releases with out->release(out).
 *   columns:    NULL (with n_columns 0) for every column, or n_columns top-level column names
 *               (NUL-terminated UTF-8): the stream has those columns, in that order;
 *   batch_size: rows per batch at most, 0 for TPR_DEFAULT_BATCH_SIZE.
 * The stream's schema is the file's Arrow schema (projected), with its metadata. Pages are
 * decoded as the stream is read: a corrupt page makes get_next return EIO, and
 * get_last_error says why. On error *out is left untouched. */
int32_t tpr_open(const char *path, const char *const *columns, size_t n_columns, int64_t batch_size,
                 struct ArrowArrayStream *out);

/* The file's row count and row group count from its footer; either pointer may be NULL. */
int32_t tpr_file_info(const char *path, int64_t *num_rows, int64_t *num_row_groups);

/* Moves the file's Arrow schema into *out (release it with out->release(out)). */
int32_t tpr_file_schema(const char *path, struct ArrowSchema *out);

/* Copies the calling thread's last tpr_ error message, NUL-terminated and truncated to cap bytes,
 * into buf. Returns the message length without the NUL (0 if the last call succeeded). */
size_t tpr_last_error(char *buf, size_t cap);

#ifdef __cplusplus
}
#endif

#endif /* TPARQUET_READ_H */
