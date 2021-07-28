#include "constants.h"
#include <boost/random.hpp>
#include <ev.h>
#include <fstream>
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
bool quiet_mode = false;
bool log_mode = false;
string recvbuf;
string sendbuf;
DBRequest request;
DBReply reply;
int sd;
std::mutex donext;
boost::random::taus88 keygen; // global key generator
boost::random::discrete_distribution<int> dist;
boost::random::uniform_int_distribution<> vdist(1, 100000000);
boost::random::uniform_int_distribution<> kdist(1, 20000000);
boost::random::taus88 vgen; // value generator
boost::random::taus88 sgen; // sequence generator
boost::random::taus88 rgen; // range generator

void stdin_cb(struct ev_loop *loop, ev_io *w, int revents);
void recv_cb(EV_P_ ev_io *w, int revents);
void prepare_cb(struct ev_loop *loop, ev_prepare *w, int revents);
ofstream fp_req;
ofstream fp_rep;
int counter;
int num;
void next(DBRequest &req) {
    req.Clear();
    counter++;
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
    if (counter > num) {
        req.set_op(OP_ERROR);
    }
}
int main(int argc, char *argv[]) {
    struct ev_loop *loop = EV_DEFAULT;
    struct sockaddr_in addr;
    ev_io stdin_watcher, recv_watcher;
    ev_prepare prepare_watcher;
    if (argc < 3) {
        fprintf(stderr, "usage %s hostname port\n", argv[0]);
        exit(0);
    }
    if (argc == 4) {
        if (0 == strcmp(argv[3], "bench")) {
            quiet_mode = true;
        }
        if (0 == strcmp(argv[3], "log")) {
            quiet_mode = false;
            log_mode = true;
            fp_req.open("db.req", ios::out);
            fp_rep.open("db.rep", ios::out);
        }
    }
    if (argc == 10) {
        if (0 == strcmp(argv[3], "bench")) {
            quiet_mode = true;
        }
        if (0 == strcmp(argv[3], "log")) {
            quiet_mode = false;
            log_mode = true;
            fp_req.open("db.req", ios::out);
            fp_rep.open("db.rep", ios::out);
        }
        vgen.seed(2021u + static_cast<unsigned int>(getpid()));
        sgen.seed(1202u + static_cast<unsigned int>(getpid()));
        rgen.seed(100u + static_cast<unsigned int>(getpid()));
        int ri = atoi(argv[4]);
        int ru = atoi(argv[5]);
        int rd = atoi(argv[6]);
        int rg = atoi(argv[7]);
        int rr = atoi(argv[8]);
        num = atoi(argv[9]);
        double probabilities[5] = {(double)ri / 10.0, (double)ru / 10.0,
                                   (double)rd / 10.0, (double)rg / 10.0,
                                   (double)rr / 10.0};
        dist = boost::random::discrete_distribution<int>(probabilities);
    }
    // Create client socket
    if ((sd = socket(PF_INET, SOCK_STREAM, 0)) < 0) {
        perror("socket error");
        return -1;
    }

    bzero(&addr, sizeof(addr));

    struct hostent *server = gethostbyname(argv[1]);
    if (server == NULL) {
        perror("ERROR, no such host\n");
        return -1;
    }

    addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr, (char *)&addr.sin_addr.s_addr,
          server->h_length);
    addr.sin_port = htons(atoi(argv[2]));

    // Connect to server socket
    if (connect(sd, (struct sockaddr *)&addr, sizeof addr) < 0) {
        perror("Connect error");
        return -1;
    }
    printf("connected successfully!\n");
    if (argc < 5) {
        ev_io_init(&stdin_watcher, stdin_cb, 0, EV_READ);
        ev_io_start(loop, &stdin_watcher);
    }
    if (argc == 10) {
        ev_prepare_init(&prepare_watcher, prepare_cb);
        ev_prepare_start(loop, &prepare_watcher);
    }
    ev_io_init(&recv_watcher, recv_cb, sd, EV_READ);
    ev_io_start(loop, &recv_watcher);

    // It will ask the operating system for any new events,
    // call the watcher callbacks, and then repeat the whole process
    // indefinitely
    ev_run(loop, 0);

    return 0;
}
// Read input from user and send message to the server
DBRequest req;
void prepare_cb(struct ev_loop *loop, ev_prepare *w, int revents) {
    char sbuf[1024];
    next(req);
    req.SerializeToArray(&sbuf, sizeof(sbuf));
    send(sd, &sbuf, sizeof(sbuf), 0);
    if (req.op() == OP_ERROR) {
        fp_req << "quit" << endl;
        fp_req.flush();
    } else if (req.op() != OP_DELETE && req.op() != OP_GET) {
        fp_req << dict[req.op()].c_str() << " " << req.key1() << " "
               << req.key2() << endl;
    } else {
        fp_req << dict[req.op()].c_str() << " " << req.key1() << endl;
    }
}
void stdin_cb(struct ev_loop *loop, ev_io *w, int revents) {
    char sbuf[1024];
    char op[6];
    int a;
    int64_t b, c = 0;
    if (scanf("%s", op) == -1) {
        // ev_break(EV_A_ EVBREAK_ALL);
        // close(sd);
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
    // if (b % 1000000 == 0) {
    //     printf("%ld\n", b);
    // }
    if (0 == strcmp(op, "quit")) {
        if (log_mode)
            fp_req << "quit" << endl;
        // this causes all nested ev_run's to stop iterating
        if (!log_mode)
            exit(0);
    } else {
        request.Clear();
        request.set_op(a);
        request.set_key1(b);
        request.set_key2(c);
        if (log_mode) {
            if (a == OP_ERROR) {
                fp_req.flush();
            } else if (a != OP_DELETE && a != OP_GET) {
                fp_req << dict[a].c_str() << " " << b << " " << c << endl;
            } else {
                fp_req << dict[a].c_str() << " " << b << endl;
            }
        }
        request.SerializeToArray(&sbuf, sizeof(sbuf));
        send(sd, &sbuf, sizeof(sbuf), 0);
        if (a == OP_ERROR)
            if (!log_mode)
                exit(0);
    }
}

// Receive message from the server
void recv_cb(EV_P_ ev_io *w, int revents) {
    char rbuf[1024];
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
        fp_rep.flush();
        ev_break(EV_A_ EVBREAK_ALL);
        close(sd);
        perror("server might close");
    } else {
        reply.ParseFromArray(&rbuf, sizeof(rbuf));
        if (log_mode) {
            fp_rep << "Query ";
            if (reply.result() == true) {
                fp_rep << "OK ";
            } else {
                fp_rep << "Failed ";
            }
            if (!reply.values().empty()) {

                for (auto &i : reply.values()) {
                    fp_rep << i << " ";
                }
            }
            fp_rep << endl;
        }
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