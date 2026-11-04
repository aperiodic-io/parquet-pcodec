/* Links the static library from plain C and round-trips a page: `make -C rust/capi c-test`. */
#include "pparquet.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main(void) {
    enum { N = 4096 };
    double vals[N];
    for (int i = 0; i < N; i++) vals[i] = (10000 + i % 300) / 100.0;
    size_t cap = pcp_compress_bound(sizeof vals), written = 0;
    uint8_t *payload = malloc(cap);
    double back[N];
    if (pcp_codec_id() != 20547 || pcp_abi_version() != PCP_ABI_VERSION) return 1;
    if (pcp_compress((const uint8_t *)vals, sizeof vals, 5, -1, 1, payload, cap, &written) != PCP_OK) return 2;
    if (written >= sizeof vals / 4) return 3;
    if (pcp_decompress(payload, written, (uint8_t *)back, sizeof back) != PCP_OK) return 4;
    if (memcmp(vals, back, sizeof vals) != 0) return 5;
    if (pcp_decompress(payload, written, (uint8_t *)back, sizeof back - 8) != PCP_ERR_SIZE) return 6;
    char msg[128];
    if (pcp_last_error(msg, sizeof msg) == 0) return 7;
    printf("ok: %zu -> %zu bytes; last error: %s\n", sizeof vals, written, msg);
    free(payload);
    return 0;
}
