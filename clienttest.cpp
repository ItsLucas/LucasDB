#include "constants.h"
#include <boost/random.hpp>
#include <ev.h>
#include <iostream>
#include <netdb.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
using namespace std;
#define BUFFER_SIZE 1024
#include "LucasDB.pb.h"
using namespace lucasdb;
bool quiet_mode = true;
string recvbuf;
string sendbuf;
DBRequest request;
DBReply reply;
int sd;
boost::random::taus88 keygen; // global key generator
boost::random::discrete_distribution<int> dist;
boost::random::uniform_int_distribution<> vdist(1, 100000000);
boost::random::uniform_int_distribution<> kdist(1, 20000000);
boost::random::taus88 vgen; // value generator
boost::random::taus88 sgen; // sequence generator
boost::random::taus88 rgen; // range generator
void stdin_cb(struct ev_loop *loop, ev_io *w, int revents);
void recv_cb(EV_P_ ev_io *w, int revents);
int counter;
// Generate next DBRequest
void next(DBRequest &req) {
    counter++;
    if (counter % 100000 == 0)
        std::cout << counter << std::endl;
    int nextop = dist(sgen); // 0 1 2 3
    req.set_op(nextop);
    switch (nextop) {
    case OP_INSERT: {
        req.set_key1(kdist(keygen));
        req.set_key2(vdist(vgen));
        break;
    }
    case OP_UPDATE: {
        req.set_key1(kdist(keygen));
        req.set_key2(vdist(vgen));
        break;
    }
    case OP_DELETE: {
        req.set_key1(kdist(keygen));
        break;
    }
    case OP_GET: {
        req.set_key1(kdist(keygen));
        break;
    }
    case OP_RANGE: {
        req.set_key1(kdist(rgen));
        req.set_key2(req.key1() + 100);
        break;
    }
    default:
        break;
    }
    if (counter > 6000000)
        req.set_op(OP_ERROR);
}
int main(int argc, char *argv[]) {
    struct ev_loop *loop = EV_DEFAULT;
    struct sockaddr_in addr;
    ev_io stdin_watcher, recv_watcher;

    if (argc < 3) {
        fprintf(stderr, "usage %s hostname port\n", argv[0]);
        exit(0);
    }
    if (argc == 4) {
        if (0 == strcmp(argv[3], "bench")) {
            quiet_mode = true;
        }
    }
    // Create client socket
    vgen.seed(2021u + static_cast<unsigned int>(getpid()));
    sgen.seed(1202u + static_cast<unsigned int>(getpid()));
    rgen.seed(100u + static_cast<unsigned int>(getpid()));
    int ri = atoi(argv[3]);
    int ru = atoi(argv[4]);
    int rd = atoi(argv[5]);
    int rg = atoi(argv[6]);
    int rr = atoi(argv[7]);
    double probabilities[5] = {(double)ri / 10.0, (double)ru / 10.0,
                               (double)rd / 10.0, (double)rg / 10.0,
                               (double)rr / 10.0};
    dist = boost::random::discrete_distribution<int>(probabilities);
    // Create client socket
    if ((sd = socket(PF_INET, SOCK_STREAM, 0)) < 0) {
        perror("socket error");
        return -1;
    }

    bzero(&addr, sizeof(addr));

    struct hostent *server = gethostbyname("127.0.0.1");
    if (server == NULL) {
        perror("ERROR, no such host\n");
        return -1;
    }

    addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr, (char *)&addr.sin_addr.s_addr,
          server->h_length);
    addr.sin_port = htons(atoi("8192"));

    // Connect to server socket
    if (connect(sd, (struct sockaddr *)&addr, sizeof addr) < 0) {
        perror("Connect error");
        return -1;
    }
    printf("connected successfully!\n");
    // in bench mode, send an empty message to trigger server
    DBRequest request;
    request.set_op(OP_GET);
    request.set_key1(1);
    char sbuf[1024];
    request.SerializeToArray(&sbuf, sizeof(sbuf));
    send(sd, &sbuf, sizeof(sbuf), 0);
    // ev_io_init(&stdin_watcher, stdin_cb, /*STDIN_FILENO*/ 0,
    //            EV_READ);
    // ev_io_start(loop, &stdin_watcher);

    ev_io_init(&recv_watcher, recv_cb, sd, EV_READ);
    ev_io_start(loop, &recv_watcher);

    // It will ask the operating system for any new events,
    // call the watcher callbacks, and then repeat the whole process
    // indefinitely
    ev_run(loop, 0);
    return 0;
}
// Read input from user and send message to the server

void stdin_cb(struct ev_loop *loop, ev_io *w, int revents) {
    char sbuf[1024];
    char op[6];
    int a;
    int64_t b, c = 0;
    if (scanf("%s", op) == -1) {
        ev_break(EV_A_ EVBREAK_ALL);
        close(sd);
    }
    if (0 == strcmp(op, "insert")) {
        a = OP_INSERT;
        scanf("%ld %ld", &b, &c);
    } else if (0 == strcmp(op, "update")) {
        a = OP_UPDATE;
        scanf("%ld %ld", &b, &c);
    } else if (0 == strcmp(op, "delete")) {
        scanf("%ld", &b);
        a = OP_DELETE;
    } else if (0 == strcmp(op, "get")) {
        scanf("%ld", &b);
        a = OP_GET;
    } else if (0 == strcmp(op, "range")) {
        scanf("%ld %ld", &b, &c);
        a = OP_RANGE;
    } else if (0 == strcmp(op, "slap")) {
        scanf("%ld", &b);
        pid_t pid = getpid();
        c = pid;
        a = OP_RANDINIT;
    } else {
        a = OP_ERROR;
    }
    // fflush(stdin);
    if (b % 1000000 == 0) {
        printf("%ld\n", b);
    }
    // should never reach here if quit
    if (0 == strcmp(op, "quit")) {
        // this causes all nested ev_run's to stop iterating
        exit(0);
    } else {
        request.set_op(a);
        request.set_key1(b);
        request.set_key2(c);
        request.SerializeToArray(&sbuf, sizeof(sbuf));
        send(sd, &sbuf, sizeof(sbuf), 0);
        if (a == OP_ERROR)
            exit(0);
    }
}

// Receive message from the server
DBRequest req;
// Receive message from the server
void recv_cb(EV_P_ ev_io *w, int revents) {
    char rbuf[1024];
    char sbuf[1024];
    req.Clear();
    ssize_t read;
    if (EV_ERROR & revents) {
        perror("got invalid event");
        return;
    }

    read = recv(w->fd, &rbuf, sizeof(rbuf), 0);
    if (read < 0) {
        perror("read error");
    } else if (read == 0) {
        // it will result in dead loop if we don't stop ev loop
        ev_break(EV_A_ EVBREAK_ALL);
        close(sd);
        perror("server might close");
    } else {
        reply.ParseFromArray(&rbuf, sizeof(rbuf));

        if (!quiet_mode) {
            printf("Query ");
            if (reply.result() == true) {
                printf("OK ");
            } else {
                printf("Failed ");
            }
            if (!reply.values().empty()) {

                for (auto &i : reply.values()) {
                    printf("%u ", i);
                }
            }
            printf("\n");
        }
        next(req);
        if (req.op() == OP_ERROR)
            exit(0);
        request.SerializeToArray(&sbuf, sizeof(sbuf));
        send(sd, &sbuf, sizeof(sbuf), 0);
    }
}