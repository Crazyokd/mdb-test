#include "util.h"

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void u16_to_hex_string(unsigned char *in, char *out)
{
    out[0] = '0';
    out[1] = 'x';
    for (int i = 1; i < 16; i++) {
        sprintf(out + i * 2, "%02x", in[i - 1]);
    }
    out[34] = 0;
}

static unsigned int DEKHash(const unsigned char *str, unsigned int len)
{
    unsigned int hash = len;
    unsigned int i = 0;

    for (i = 0; i < len; str++, i++) {
        hash = ((hash << 5) ^ (hash >> 27)) ^ (*str);
    }
    return hash;
}

int calculate_queue_no(char *src_ip, unsigned short src_port,
                       char *dst_ip, unsigned short dst_port, int queue_num)
{
    /* perf: do not hash as much as possible */
    if (queue_num < 2) return 0;

    unsigned int queue_id = 0;

    char result[272] = {0}; /* 256 + 16 */
    char src[136] = {0}; /* 128 + 8 */
    char dst[136] = {0}; /* 128 + 8 */
    snprintf(src, sizeof(src) - 1, "%s%hu", src_ip, src_port); // %hu: unsigned short int
    snprintf(dst, sizeof(dst) - 1, "%s%hu", dst_ip, dst_port);
    if (strcmp(src, dst) > 0) {
        snprintf(result, sizeof(result) - 1, "%s%s", src, dst);
    } else {
        snprintf(result, sizeof(result) - 1, "%s%s", dst, src);
    }

    queue_id = DEKHash((unsigned char *)result, strlen(result));

    return queue_id % queue_num;
}

static FILE *g_lol_file = NULL;
static lol_level_e g_lol_level = LOL_LOG_INFO;

void lol_log(lol_level_e log_level, const char *fmtspec, ...)
{
    if (log_level > g_lol_level) return;
    va_list arglist;

    if (0 == g_lol_file) {
        g_lol_file = stdout;
    }

    va_start(arglist, fmtspec);

    vfprintf(g_lol_file, fmtspec, arglist);

    va_end(arglist);

    fflush(g_lol_file);
}