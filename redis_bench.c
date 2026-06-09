/*
 * redis_bench.c — Redis client with TCP_QUEUE_STATE snapshots
 *
 * Single thread, single socket, controlled RPS.
 * Supports SET-only and 95/5 SET/GET workloads.
 * SIGUSR1 triggers queue state snapshot to /tmp/qstate_N.txt
 *
 * Build:
 *   gcc -O2 -o redis_bench redis_bench.c -static
 *
 * Usage:
 *   ./redis_bench <server_ip> <port> <rps> <duration_sec> <workload> <nagle>
 *
 *   workload: "set" (100% SET) or "mixed" (95% SET, 5% GET)
 *   nagle:    "on" or "off"
 *
 * Example:
 *   taskset -c 1 ./redis_bench 10.0.0.1 6379 5000 15 set off
 */

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <time.h>
#include <errno.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <stdint.h>

#ifndef TCP_QUEUE_STATE
#define TCP_QUEUE_STATE 100
#endif

struct tcp_queue_state_snapshot {
    uint64_t unacked_time_ns;
    uint64_t unacked_integral;
    uint64_t unacked_total;
    int64_t  unacked_size;
    uint64_t unread_time_ns;
    uint64_t unread_integral;
    uint64_t unread_total;
    int64_t  unread_size;
    uint64_t ackdelay_time_ns;
    uint64_t ackdelay_integral;
    uint64_t ackdelay_total;
    int64_t  ackdelay_size;
};

static volatile int snapshot_requested = 0;
static int snapshot_count = 0;
static int sock_fd = -1;
static long completed_requests = 0;

static void sigusr1_handler(int sig) {
    (void)sig;
    snapshot_requested = 1;
}

static void take_snapshot(void) {
    if (!snapshot_requested) return;
    snapshot_requested = 0;

    struct tcp_queue_state_snapshot snap;
    socklen_t optlen = sizeof(snap);
    memset(&snap, 0, sizeof(snap));
    int ret = getsockopt(sock_fd, IPPROTO_TCP, TCP_QUEUE_STATE, &snap, &optlen);
    if (ret < 0) {
        fprintf(stderr, "ERROR: getsockopt(TCP_QUEUE_STATE) failed: %s\n", strerror(errno));
        _exit(1);
    }
    if (optlen != sizeof(snap)) {
        fprintf(stderr, "ERROR: getsockopt returned %u bytes, expected %zu\n", optlen, sizeof(snap));
        _exit(1);
    }

    char fname[64];
    snprintf(fname, sizeof(fname), "/tmp/qstate_%d.txt", snapshot_count++);
    FILE *f = fopen(fname, "w");
    if (!f) {
        perror("fopen snapshot");
        _exit(1);
    }
    fprintf(f, "%lu %lu %lu %ld %lu %lu %lu %ld %lu %lu %lu %ld %ld\n",
        (unsigned long)snap.unacked_time_ns, (unsigned long)snap.unacked_integral,
        (unsigned long)snap.unacked_total, (long)snap.unacked_size,
        (unsigned long)snap.unread_time_ns, (unsigned long)snap.unread_integral,
        (unsigned long)snap.unread_total, (long)snap.unread_size,
        (unsigned long)snap.ackdelay_time_ns, (unsigned long)snap.ackdelay_integral,
        (unsigned long)snap.ackdelay_total, (long)snap.ackdelay_size,
        completed_requests);
    fclose(f);
}

static long time_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000000000L + ts.tv_nsec;
}

/* Write with EINTR retry */
static int write_all(int fd, const char *buf, int len) {
    int sent = 0;
    while (sent < len) {
        int ret = write(fd, buf + sent, len - sent);
        if (ret < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        sent += ret;
    }
    return sent;
}

/* Read with EINTR retry, reads until we have at least 'need' bytes or find pattern */
static char recvbuf[65536];
static int recvbuf_len = 0;

static int read_response(int fd) {
    /* Redis responses end with \r\n. For bulk strings: $<len>\r\n<data>\r\n
     * For simple strings/errors/integers: +/-/: ...\r\n */
    while (1) {
        /* Check if we have a complete response */
        if (recvbuf_len > 0) {
            if (recvbuf[0] == '+' || recvbuf[0] == '-' || recvbuf[0] == ':') {
                /* Simple response: find \r\n */
                char *crlf = memmem(recvbuf, recvbuf_len, "\r\n", 2);
                if (crlf) {
                    int consumed = (crlf - recvbuf) + 2;
                    recvbuf_len -= consumed;
                    if (recvbuf_len > 0) memmove(recvbuf, recvbuf + consumed, recvbuf_len);
                    return 0;
                }
            } else if (recvbuf[0] == '$') {
                /* Bulk string: $<len>\r\n<data>\r\n */
                char *crlf = memmem(recvbuf, recvbuf_len, "\r\n", 2);
                if (crlf) {
                    int hdr_len = (crlf - recvbuf) + 2;
                    int body_len = atoi(recvbuf + 1);
                    if (body_len < 0) {
                        /* $-1\r\n (nil) */
                        recvbuf_len -= hdr_len;
                        if (recvbuf_len > 0) memmove(recvbuf, recvbuf + hdr_len, recvbuf_len);
                        return 0;
                    }
                    int total_need = hdr_len + body_len + 2;
                    if (recvbuf_len >= total_need) {
                        recvbuf_len -= total_need;
                        if (recvbuf_len > 0) memmove(recvbuf, recvbuf + total_need, recvbuf_len);
                        return 0;
                    }
                }
            }
        }

        int ret = read(fd, recvbuf + recvbuf_len, sizeof(recvbuf) - recvbuf_len);
        if (ret < 0) {
            if (errno == EINTR) { take_snapshot(); continue; }
            return -1;
        }
        if (ret == 0) return -1;
        recvbuf_len += ret;
    }
}

/* 16-byte key */
static const char KEY[] = "0000000000000000";
/* 16KB value (filled with 'x') */
static char VALUE[16384];

static int send_set(int fd) {
    /* SET 0000000000000000 <16384 bytes>\r\n in RESP:
     * *3\r\n$3\r\nSET\r\n$16\r\n0000000000000000\r\n$16384\r\n<value>\r\n */
    static char cmd[16384 + 128];
    int len = snprintf(cmd, sizeof(cmd),
        "*3\r\n$3\r\nSET\r\n$16\r\n%s\r\n$16384\r\n", KEY);
    memcpy(cmd + len, VALUE, 16384);
    len += 16384;
    cmd[len++] = '\r';
    cmd[len++] = '\n';
    return write_all(fd, cmd, len);
}

static int send_get(int fd) {
    /* GET 0000000000000000 in RESP:
     * *2\r\n$3\r\nGET\r\n$16\r\n0000000000000000\r\n */
    static const char cmd[] = "*2\r\n$3\r\nGET\r\n$16\r\n0000000000000000\r\n";
    return write_all(fd, cmd, sizeof(cmd) - 1);
}

int main(int argc, char **argv) {
    if (argc != 7) {
        fprintf(stderr, "Usage: %s <ip> <port> <rps> <duration_sec> <set|mixed> <on|off>\n", argv[0]);
        return 1;
    }

    const char *ip = argv[1];
    int port = atoi(argv[2]);
    int rps = atoi(argv[3]);
    int duration = atoi(argv[4]);
    int mixed = (strcmp(argv[5], "mixed") == 0);
    int nagle_on = (strcmp(argv[6], "on") == 0);

    memset(VALUE, 'x', sizeof(VALUE));

    /* Connect */
    sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_fd < 0) { perror("socket"); return 1; }

    struct sockaddr_in addr = { .sin_family = AF_INET, .sin_port = htons(port) };
    inet_pton(AF_INET, ip, &addr.sin_addr);
    if (connect(sock_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect"); return 1;
    }

    /* Nagle control */
    if (!nagle_on) {
        int one = 1;
        setsockopt(sock_fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    }
    /* If nagle_on, TCP_NODELAY is off by default — nothing to do */

    signal(SIGUSR1, sigusr1_handler);

    fprintf(stderr, "PID %d | %s:%d | %d RPS | %ds | workload=%s | nagle=%s\n",
            getpid(), ip, port, rps, duration, mixed ? "mixed" : "set",
            nagle_on ? "on" : "off");

    long interval_ns = 1000000000L / rps;
    long start = time_ns();
    long end = start + (long)duration * 1000000000L;
    long next_tx = start;
    unsigned int req_num = 0;

    while (1) {
        long now = time_ns();
        if (now >= end) break;

        take_snapshot();

        if (now < next_tx) {
            /* Sleep-based pacing: sleep until next send time */
            struct timespec sl;
            long wait = next_tx - now;
            if (wait > 1000000) { /* only sleep if > 1ms, else busy-wait */
                sl.tv_sec = 0;
                sl.tv_nsec = wait > 5000000 ? 5000000 : wait; /* cap at 5ms */
                nanosleep(&sl, NULL);
            }
            continue;
        }

        /* Send request */
        int ret;
        if (mixed && (req_num % 20 == 0)) {
            /* 5% GET (1 out of 20) */
            ret = send_get(sock_fd);
        } else {
            ret = send_set(sock_fd);
        }
        if (ret < 0) { perror("send"); break; }

        /* Read response */
        if (read_response(sock_fd) < 0) { perror("read_response"); break; }

        completed_requests++;
        req_num++;
        next_tx += interval_ns;
    }

    long elapsed_ns = time_ns() - start;
    fprintf(stderr, "Done: %ld reqs in %.2fs = %.0f RPS\n",
            completed_requests, elapsed_ns / 1e9,
            (double)completed_requests / (elapsed_ns / 1e9));

    close(sock_fd);
    return 0;
}
