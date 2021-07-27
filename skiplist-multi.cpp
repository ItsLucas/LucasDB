#include "LucasDB.pb.h"
#include "skiplist.h"
#include <boost/random.hpp>
#include <evpp/buffer.h>
#include <evpp/event_loop_thread_pool.h>
#include <evpp/tcp_conn.h>
#include <evpp/tcp_server.h>
using namespace SL;
boost::random::uniform_int_distribution<> vdist(1, 100000000);
boost::random::taus88 vgen; // value generator
static constexpr bool use_persistant = false;
SkipList<int64_t, uint32_t> skipList(32, use_persistant);
using namespace lucasdb;
bool do_insert(int64_t key1, int64_t value) {
    // printf("Inserting key: %ld, value: %ld\n", key1, value);
    if (use_persistant) { // special treatment in server side, the lock is so
                          // confusing.
        int tmp = skipList.search(key1);
        if (tmp != -1)
            return false;
    }
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
    vgen.seed(seed);
    printf("Begin slapping database with %ld entries\n", sz);
    auto start = high_resolution_clock::now();
    for (int i = 1; i <= sz; i++) {
        skipList.insert(i, vdist(vgen));
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
void OnMessage(const evpp::TCPConnPtr &conn, evpp::Buffer *msg) {
    DBRequest req;
    req.ParsePartialFromArray(msg->data(), sizeof(msg->data()));
    DBReply reply;
    char buff[1024];
    evpp::Buffer sendbuf;
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
    case OP_MUL2: {
        do_get(req.key1(), reply);
        if (reply.result())
            do_update(req.key1(), reply.values(0) * 2);
        break;
    }
    case OP_ERROR: {
        reply.set_result(false);
        conn->Close();
        return;
        break;
    }
    default:
        break;
    }
    reply.SerializePartialToArray(&buff, sizeof(buff));
    sendbuf.Append(&buff, sizeof(buff));
    conn->Send(sendbuf.data(), sendbuf.size());
    sendbuf.Reset();
    msg->Reset();
}

int main(int argc, char *argv[]) {
    std::string addr = "0.0.0.0:9099";
    uint32_t thread_num = 8;

    if (argc != 1 && argc != 3) {
        printf("Usage: %s <port> <thread-num>\n", argv[0]);
        printf("  e.g: %s 9099 12\n", argv[0]);
        return 0;
    }

    if (argc == 3) {
        addr = std::string("0.0.0.0:") + argv[1];
        thread_num = atoi(argv[2]);
    }

    evpp::EventLoop loop;
    evpp::EventLoopThreadPool tpool(&loop, thread_num);
    tpool.Start(true);

    std::vector<std::shared_ptr<evpp::TCPServer>> tcp_servers;
    for (uint32_t i = 0; i < thread_num; i++) {
        evpp::EventLoop *next = tpool.GetNextLoop();
        std::shared_ptr<evpp::TCPServer> s(
            new evpp::TCPServer(next, addr, std::to_string(i) + "#server", 0));
        s->SetMessageCallback(&OnMessage);
        s->Init();
        s->Start();
        tcp_servers.push_back(s);
    }

    loop.Run();
    return 0;
}