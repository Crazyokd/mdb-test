#ifndef _UTIL_H
#define _UTIL_H

void u16_to_hex_string(unsigned char *in, char *out);
int calculate_queue_no(char *src_ip, unsigned short src_port,
                       char *dst_ip, unsigned short dst_port, int queue_num);

#define WRITE_IO_TO_WORKER(name, tdr, src_ip, src_port, dst_ip, dst_port) \
do {                                                                      \
    IOConnector *ioc = getIOConnector(name);                              \
    unsigned int q_no = calculate_queue_no(src_ip, src_port, dst_ip,      \
                                           dst_port, ioc->q_num);         \
    if (sprtWriteIO(ioc, q_no, tdr)) delete tdr;                          \
} while (0);

typedef enum lol_level {
    LOL_LOG_NONE = 0,
    LOL_LOG_FATAL,
    LOL_LOG_ERROR,
    LOL_LOG_WARN,
    LOL_LOG_INFO,
    LOL_LOG_DEBUG,
    LOL_LOG_TRACE,
    LOL_LOG_DEFAULT = LOL_LOG_INFO,
    LOL_LOG_FULL = LOL_LOG_TRACE,
} lol_level_e;

void lol_log(lol_level_e log_level, const char * fmtspec, ...);

#define lol_fatal(...) lol_log(LOL_LOG_FATAL, __VA_ARGS__)
#define lol_error(...) lol_log(LOL_LOG_ERROR, __VA_ARGS__)
#define lol_warn(...)  lol_log(LOL_LOG_WARN,  __VA_ARGS__)
#define lol_info(...)  lol_log(LOL_LOG_INFO,  __VA_ARGS__)
#define lol_debug(...) lol_log(LOL_LOG_DEBUG, __VA_ARGS__)
#define lol_trace(...) lol_log(LOL_LOG_TRACE, __VA_ARGS__)

#endif
