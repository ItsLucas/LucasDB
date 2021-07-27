#include "LucasDB.pb.h"
#include "constants.h"
#include <boost/random.hpp>
#include <evpp/buffer.h>
#include <evpp/event_loop_thread_pool.h>
#include <evpp/tcp_client.h>
#include <evpp/tcp_conn.h>
int ri, ru, rd, rg, rr;
bool logged = false;
bool quiet_mode = true;
using namespace lucasdb;
boost::random::taus88 keygen; // global key generator
boost::random::discrete_distribution<int> dist;
boost::random::uniform_int_distribution<> vdist(1, 100000000);
boost::random::uniform_int_distribution<> kdist(1, 20000000);
thread_local boost::random::taus88 vgen; // value generator
thread_local boost::random::taus88 sgen; // sequence generator
thread_local boost::random::taus88 rgen; // range generator
int counter;
DBRequest next() {
    counter++;
    if (counter % 100000 == 0)
        std::cout << counter << std::endl;
    int nextop = dist(sgen); // 0 1 2 3
    DBRequest req;
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
        req.set_key1(kdist(rgen));
        break;
    }
    default:
        break;
    }
    return req;
}
thread_local DBReply reply;
void OnMessage(const evpp::TCPConnPtr &conn, evpp::Buffer *msg) {
    char rbuf[128];
    reply.ParseFromArray(msg->data(), sizeof(msg->data()));
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
    DBRequest req = next();
    req.SerializePartialToArray(rbuf, sizeof(rbuf));
    msg->Reset();
    msg->Append(rbuf, sizeof(rbuf));
    conn->Send(msg);
    msg->Reset();
}

int main(int argc, char *argv[]) {
    std::string addr = "0.0.0.0:9099";
    uint32_t thread_num = 2;

    if (argc != 1 && argc != 3 && argc != 7 && argc != 8) {
        printf("Usage: %s <port> <thread-num>\n", argv[0]);
        printf("  e.g: %s 9099 12\n", argv[0]);
        return 0;
    }

    if (argc == 3) {
        addr = std::string("0.0.0.0:") + argv[1];
        thread_num = atoi(argv[2]);
    }
    if (argc == 8) {
        addr = std::string("0.0.0.0:") + argv[1];
        thread_num = atoi(argv[2]);
        ri = atoi(argv[3]);
        ru = atoi(argv[4]);
        rd = atoi(argv[5]);
        rg = atoi(argv[6]);
        rr = atoi(argv[7]);
        double probabilities[5] = {(double)ri / 10.0, (double)ru / 10.0,
                                   (double)rd / 10.0, (double)rg / 10.0,
                                   (double)rr / 10.0};
        dist = boost::random::discrete_distribution<int>(probabilities);
    }
    if (argc == 9) {
        addr = std::string("0.0.0.0:") + argv[1];
        thread_num = atoi(argv[2]);
        ri = atoi(argv[3]);
        ru = atoi(argv[4]);
        rd = atoi(argv[5]);
        rg = atoi(argv[6]);
        rr = atoi(argv[7]);
        double probabilities[5] = {(double)ri / 10.0, (double)ru / 10.0,
                                   (double)rd / 10.0, (double)rg / 10.0,
                                   (double)rr / 10.0};
        dist = boost::random::discrete_distribution<int>(probabilities);
        logged = true;
    }

    evpp::EventLoop loop;
    evpp::EventLoopThreadPool tpool(&loop, thread_num);
    tpool.Start(true);

    std::vector<std::shared_ptr<evpp::TCPClient>> tcp_clients;
    for (uint32_t i = 0; i < thread_num; i++) {
        evpp::EventLoop *next = tpool.GetNextLoop();
        std::shared_ptr<evpp::TCPClient> c(
            new evpp::TCPClient(next, addr, std::to_string(i) + "#client"));
        c->SetMessageCallback(&OnMessage);
        c->Connect();
        c->SetConnectionCallback([](const evpp::TCPConnPtr &conn) {
            if (conn->IsConnected()) {
                DBRequest req;
                req.set_op(OP_GET);
                req.set_key1(1);
                char rbuf[128];
                req.SerializePartialToArray(rbuf, sizeof(rbuf));
                evpp::Buffer msg;
                msg.Append(rbuf, sizeof(rbuf));
                conn->Send(&msg);
                msg.Reset();
            } else {
                conn->loop()->Stop();
            }
        });
        tcp_clients.push_back(c);
    }

    loop.Run();
    return 0;
}

#ifdef WIN32
#include "../echo/tcpecho/winmain-inl.h"
#endif