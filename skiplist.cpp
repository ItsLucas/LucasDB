#include "skiplist.h"
#include "LucasDB.pb.h"
#include <errno.h>
#include <ev++.h>
#include <fcntl.h>
#include <list>
#include <netinet/in.h>
#include <resolv.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
//
//   Buffer class - allow for output buffering such that it can be written out
//                                 into async pieces
//
using namespace SL;
SkipList<int64_t, uint32_t> skipList(25, false);
using namespace lucasdb;

//
//   A single instance of a non-blocking Echo handler
//

bool do_insert(int64_t key1, int64_t value) {
    // printf("Inserting key: %ld, value: %ld\n", key1, value);
    return skipList.insert(key1, (uint32_t)value);
}

bool do_update(int64_t key1, int64_t value) {
    return skipList.update(key1, (uint32_t)value);
}

bool do_delete(int64_t key1) { return skipList.remove(key1); }

void do_get(int64_t key1, DBReply &reply) {
    int tmp = skipList.search(key1);
    if (tmp != -1) {
        reply.set_result(tmp != -1);
        reply.add_values(tmp);
    } else {
        reply.set_result(false);
    }
}
using namespace std::chrono;
void slap_db(int64_t sz, unsigned int seed) {
    srand(seed);
    printf("Begin slapping database with %ld entries\n", sz);
    auto start = high_resolution_clock::now();
    for (int i = 1; i <= sz; i++) {
        skipList.insert(i, rand());
    }
    auto stop = high_resolution_clock::now();
    auto duration = duration_cast<milliseconds>(stop - start);

    // To get the value of duration use the count()
    // member function on the duration object
    printf("slap completed, time elapsed: %ldms, QPS: %lf/s\n",
           duration.count(), (double)sz / (double)duration.count() * 1000);
}
vector<uint32_t> do_range(int64_t key1, int64_t key2) {
    return skipList.range(key1, key2);
}

class EchoInstance {
  private:
    ev::io io;
    static int total_clients;
    int sfd;
    // Buffers that are pending write
    std::list<DBReply> write_queue;

    // Generic callback
    void callback(ev::io &watcher, int revents) {
        if (EV_ERROR & revents) {
            perror("got invalid event");
            return;
        }

        if (revents & EV_READ)
            read_cb(watcher);

        if (revents & EV_WRITE)
            write_cb(watcher);

        if (write_queue.empty()) {
            io.set(ev::READ);
        } else {
            io.set(ev::READ | ev::WRITE);
        }
    }

    // Socket is writable
    void write_cb(ev::io &watcher) {
        char sendbuf[1024];
        if (write_queue.empty()) {
            io.set(ev::READ);
            return;
        }

        DBReply reply = write_queue.front();
        memset(sendbuf, 0, sizeof(sendbuf));
        // printf("[DEBUG] Writing reply value_len=%u "
        //        "result=%d\n",
        //        reply.values().size(), reply.result());
        reply.SerializeToArray(&sendbuf, sizeof(sendbuf));
        ssize_t written = write(watcher.fd, &sendbuf, sizeof(sendbuf));
        if (written < 0) {
            io.stop();
        }

        write_queue.pop_front();
    }

    // Receive message from client socket
    void read_cb(ev::io &watcher) {
        char buffer[1024];

        ssize_t nread = recv(watcher.fd, buffer, sizeof(buffer), 0);

        if (nread < 0) {
            printf("read error\n");
            perror("read error");
            return;
        }

        if (nread == 0) {
            // Gack - we're deleting ourself inside of ourself!
            delete this;
        } else {
            // Send message back to the client
            DBRequest req;
            req.ParsePartialFromArray(&buffer, sizeof(buffer));
            // printf("[DEBUG] pBuf op=%d key1=%lld key2=%u value=%u
            // result=%d\n",
            //        req.op(), req.key1(), req.key2());
            DBReply reply;
            switch (req.op()) {
            case OP_INSERT: {
                reply.set_result(do_insert(req.key1(), req.key2()));
                break;
            }
            case OP_UPDATE: {
                reply.set_result(do_update(req.key1(), req.key2()));
                break;
            }
            case OP_DELETE: {
                reply.set_result(do_delete(req.key1()));
                break;
            }
            case OP_GET: {
                do_get(req.key1(), reply);
                break;
            }
            case OP_RANGE: {
                reply.set_result(true);
                for (auto &i : do_range(req.key1(), req.key2())) {
                    reply.add_values(i);
                }
                break;
            }
            case OP_RANDINIT: {
                reply.set_result(true);
                slap_db(req.key1(), req.key2());
                break;
            }
            case OP_ERROR: {
                reply.set_result(false);
                io.stop();

                break;
            }
            default:
                break;
            }
            write_queue.push_back(reply);
        }
    }

    // effictivly a close and a destroy
    virtual ~EchoInstance() {
        // Stop and free watcher if client socket is closing
        io.stop();

        close(sfd);

        printf("%d client(s) connected.\n", --total_clients);
    }

  public:
    EchoInstance(int s) : sfd(s) {
        fcntl(s, F_SETFL, fcntl(s, F_GETFL, 0) | O_NONBLOCK);

        printf("Got connection\n");
        total_clients++;

        io.set<EchoInstance, &EchoInstance::callback>(this);

        io.start(s, ev::READ);
    }
};

class EchoServer {
  private:
    ev::io io;
    ev::sig sio;
    int s;

  public:
    void io_accept(ev::io &watcher, int revents) {
        if (EV_ERROR & revents) {
            perror("got invalid event");
            return;
        }

        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);

        int client_sd =
            accept(watcher.fd, (struct sockaddr *)&client_addr, &client_len);

        if (client_sd < 0) {
            perror("accept error");
            return;
        }

        EchoInstance *client = new EchoInstance(client_sd);
    }

    static void signal_cb(ev::sig &signal, int revents) {
        signal.loop.break_loop();
    }

    EchoServer(int port) {
        printf("Listening on port %d\n", port);

        struct sockaddr_in addr;

        s = socket(PF_INET, SOCK_STREAM, 0);

        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        addr.sin_addr.s_addr = INADDR_ANY;

        if (bind(s, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
            perror("bind");
        }

        fcntl(s, F_SETFL, fcntl(s, F_GETFL, 0) | O_NONBLOCK);

        listen(s, 5);

        io.set<EchoServer, &EchoServer::io_accept>(this);
        io.start(s, ev::READ);

        sio.set<&EchoServer::signal_cb>();
        sio.start(SIGINT);
    }

    virtual ~EchoServer() {
        shutdown(s, SHUT_RDWR);
        close(s);
    }
};

int EchoInstance::total_clients = 0;

int main(int argc, char **argv) {
    int port = 8192;

    if (argc > 1)
        port = atoi(argv[1]);

    ev::default_loop loop;
    EchoServer echo(port);

    loop.run(0);

    return 0;
}