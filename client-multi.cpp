#include "constants.h"
#include <boost/random.hpp>
#include <ev.h>
#include <iostream>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>
using namespace std;
#define BUFFER_SIZE 1024
#define MAX_CHILD_CNT 16
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
boost::random::taus88 ggen;
int retval[MAX_CHILD_CNT];
void prepare_cb(struct ev_loop *loop, ev_prepare *w, int revents);
void recv_cb(EV_P_ ev_io *w, int revents);
int counter;
// Generate next DBRequest
void next(DBRequest &req) {
    counter++;
    if (counter % 100000 == 0)
        printf("%d\n", counter);
    int nextop = dist(sgen); // 0 1 2 3
    req.set_op(nextop);
    switch (nextop) {
    case OP_INSERT: {
        req.set_key1(kdist(keygen));
        req.set_key2(vdist(vgen));
        break;
    }
    case OP_UPDATE: {
        req.set_op(OP_MUL2);
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
    if (counter > 500000) {
        req.set_op(OP_ERROR);
    }
}
// this is benchmarking client. Only random operations are available.
int main(int argc, char *argv[]) {
    pid_t child_pid, wpid;
    int status = 0;
    struct ev_loop *loop = EV_DEFAULT;
    struct sockaddr_in addr;
    ev_prepare prepare_watcher;
    ev_io recv_watcher;
    int exit_status;
    if (argc != 7) {
        fprintf(stderr, "usage %s thread_num <five operations>\n", argv[0]);
        exit(0);
    }
    if (argc == 7) {
        int child_cnt = atoi(argv[1]);
        if (child_cnt > MAX_CHILD_CNT) {
            perror("Thread limit exceeded design");
            return -1;
        }
        printf("starting %d workers\n", child_cnt);
        for (int id = 0; id < child_cnt; id++) {
            if ((child_pid = fork()) == 0) {
                // init RNGs
                vgen.seed(2021u + static_cast<unsigned int>(getpid()));
                sgen.seed(1202u + static_cast<unsigned int>(getpid()));
                rgen.seed(100u + static_cast<unsigned int>(getpid()));
                int ri = atoi(argv[2]);
                int ru = atoi(argv[3]);
                int rd = atoi(argv[4]);
                int rg = atoi(argv[5]);
                int rr = atoi(argv[6]);
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
                int flag = 1;
                int ret = setsockopt(sd, IPPROTO_TCP, TCP_NODELAY,
                                     (char *)&flag, sizeof(flag));
                if (ret == -1) {
                    printf("Couldn't setsockopt(TCP_NODELAY)\n");
                    exit(-1);
                }
                // Connect to server socket
                if (connect(sd, (struct sockaddr *)&addr, sizeof addr) < 0) {
                    perror("Connect error");
                    return -1;
                }
                printf("connected successfully!\n");

                // ev_io_init(&stdin_watcher, stdin_cb, /*STDIN_FILENO*/ 0,
                //            EV_READ);
                // ev_io_start(loop, &stdin_watcher);
                ev_prepare_init(&prepare_watcher, prepare_cb);
                ev_prepare_start(loop, &prepare_watcher);
                ev_io_init(&recv_watcher, recv_cb, sd, EV_READ);
                ev_io_start(loop, &recv_watcher);

                // It will ask the operating system for any new events,
                // call the watcher callbacks, and then repeat the whole process
                // indefinitely
                ev_run(loop, 0);
                exit(0);
            }
        }
    }

    while ((wpid = wait(&status)) > 0)
        ;
    return 0;
}
// Read input from user and send message to the server
DBRequest req;
void prepare_cb(struct ev_loop *loop, ev_prepare *w, int revents) {
    char sbuf[1024];
    if (ggen() % 2) {
        next(req);
        req.SerializeToArray(&sbuf, sizeof(sbuf));
        send(sd, &sbuf, sizeof(sbuf), 0);
    } else {
        next(req);
        req.SerializeToArray(&sbuf, sizeof(sbuf));
        send(sd, &sbuf, sizeof(sbuf), 0);
        next(req);
        req.SerializeToArray(&sbuf, sizeof(sbuf));
        send(sd, &sbuf, sizeof(sbuf), 0);
    }
}

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
    }
}