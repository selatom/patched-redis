#include "server.h"
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <string.h>

#ifndef TCP_QUEUE_STATE
#define TCP_QUEUE_STATE 100
#endif

struct tcp_queue_state_snapshot {
    uint64_t unacked_time_ns;
    uint64_t unacked_integral;
    uint64_t unacked_total;
    uint64_t unacked_size;
    uint64_t unread_time_ns;
    uint64_t unread_integral;
    uint64_t unread_total;
    uint64_t unread_size;
    uint64_t ackdelay_time_ns;
    uint64_t ackdelay_integral;
    uint64_t ackdelay_total;
    uint64_t ackdelay_size;
};

void queuestateCommand(client *c) {
    struct tcp_queue_state_snapshot snap;
    socklen_t optlen;
    int fd, ret;

    if (c->argc == 2) {
        fd = atoi(c->argv[1]->ptr);
        optlen = sizeof(snap);
        memset(&snap, 0, sizeof(snap));
        ret = getsockopt(fd, IPPROTO_TCP, TCP_QUEUE_STATE, &snap, &optlen);
        if (ret < 0) {
            addReplyError(c, "getsockopt TCP_QUEUE_STATE failed");
            return;
        }
        addReplyArrayLen(c, 12);
        addReplyLongLong(c, snap.unacked_time_ns);
        addReplyLongLong(c, snap.unacked_integral);
        addReplyLongLong(c, snap.unacked_total);
        addReplyLongLong(c, snap.unacked_size);
        addReplyLongLong(c, snap.unread_time_ns);
        addReplyLongLong(c, snap.unread_integral);
        addReplyLongLong(c, snap.unread_total);
        addReplyLongLong(c, snap.unread_size);
        addReplyLongLong(c, snap.ackdelay_time_ns);
        addReplyLongLong(c, snap.ackdelay_integral);
        addReplyLongLong(c, snap.ackdelay_total);
        addReplyLongLong(c, snap.ackdelay_size);
        return;
    }

    listIter li;
    listNode *ln;
    int count = 0;

    listRewind(server.clients, &li);
    while ((ln = listNext(&li)) != NULL) {
        client *cl = listNodeValue(ln);
        if (cl->conn && cl->conn->fd >= 0)
            count++;
    }

    addReplyArrayLen(c, count);

    listRewind(server.clients, &li);
    while ((ln = listNext(&li)) != NULL) {
        client *cl = listNodeValue(ln);
        if (!cl->conn) continue;
        fd = cl->conn->fd;
        if (fd < 0) continue;

        optlen = sizeof(snap);
        memset(&snap, 0, sizeof(snap));
        ret = getsockopt(fd, IPPROTO_TCP, TCP_QUEUE_STATE, &snap, &optlen);

        addReplyArrayLen(c, 13);
        addReplyLongLong(c, fd);
        addReplyLongLong(c, snap.unacked_time_ns);
        addReplyLongLong(c, snap.unacked_integral);
        addReplyLongLong(c, snap.unacked_total);
        addReplyLongLong(c, snap.unacked_size);
        addReplyLongLong(c, snap.unread_time_ns);
        addReplyLongLong(c, snap.unread_integral);
        addReplyLongLong(c, snap.unread_total);
        addReplyLongLong(c, snap.unread_size);
        addReplyLongLong(c, snap.ackdelay_time_ns);
        addReplyLongLong(c, snap.ackdelay_integral);
        addReplyLongLong(c, snap.ackdelay_total);
        addReplyLongLong(c, snap.ackdelay_size);
    }
}
