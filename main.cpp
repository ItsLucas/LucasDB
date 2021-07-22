#include "LucasDB.pb.h"
#include "skiplist.h"
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <list>
#include <queue>
#include <thread>
#include <unistd.h>
using namespace std::chrono;
using namespace std;

using namespace SL;

SkipList<int64_t, uint32_t> skipList(25, false);
// using namespace lucasdb;
// queue<DBRequest> q;
// mutex mtx;
// mutex quitguard;

// bool do_insert(int64_t key1, int64_t value) {
//     // printf("Inserting key: %ld, value: %ld\n", key1, value);
//     if (key1 % 1000000 == 0) {
//         cout << key1 << endl;
//     }
//     return skipList.insert(key1, (uint32_t)value);
// }

// bool do_update(int64_t key1, int64_t value) {
//     return skipList.update(key1, (uint32_t)value);
// }

// bool do_delete(int64_t key1) { return skipList.remove(key1); }

// void do_get(int64_t key1, DBReply &reply) {
//     int tmp = skipList.search(key1);
//     if (tmp != -1) {
//         reply.set_result(tmp != -1);
//         reply.add_values(tmp);
//     } else {
//         reply.set_result(false);
//     }
// }
// using namespace std::chrono;
// void slap_db(int64_t sz, unsigned int seed) {
//     srand(seed);
//     printf("Begin slapping database with %ld entries\n", sz);
//     auto start = high_resolution_clock::now();
//     for (int i = 1; i <= sz; i++) {
//         skipList.insert(i, rand());
//     }
//     auto stop = high_resolution_clock::now();
//     auto duration = duration_cast<milliseconds>(stop - start);

//     // To get the value of duration use the count()
//     // member function on the duration object
//     printf("slap completed, time elapsed: %ldms, QPS: %lf/s\n",
//            duration.count(), (double)sz / (double)duration.count() * 1000);
// }
// vector<uint32_t> do_range(int64_t key1, int64_t key2) {
//     return skipList.range(key1, key2);
// }
// inline void g() {
//     quitguard.lock();
//     for (;;) {
//         // cout << "acquiring lock" << endl;
//         // mtx.lock();
//         // cout << "f locked" << endl;
//         if (!q.empty()) {
//             DBRequest req = q.front();
//             // cout << req.op() << req.key1() << endl;
//             DBReply reply;
//             q.pop();
//             // mtx.unlock();
//             switch (req.op()) {
//             case OP_INSERT: {
//                 reply.set_result(do_insert(req.key1(), req.key2()));
//                 break;
//             }
//             case OP_UPDATE: {
//                 reply.set_result(do_update(req.key1(), req.key2()));
//                 break;
//             }
//             case OP_DELETE: {
//                 reply.set_result(do_delete(req.key1()));
//                 break;
//             }
//             case OP_GET: {
//                 do_get(req.key1(), reply);
//                 break;
//             }
//             case OP_RANGE: {
//                 reply.set_result(true);
//                 for (auto &i : do_range(req.key1(), req.key2())) {
//                     reply.add_values(i);
//                 }
//                 break;
//             }
//             case OP_RANDINIT: {
//                 reply.set_result(true);
//                 slap_db(req.key1(), req.key2());
//                 break;
//             }
//             case OP_ERROR: {
//                 reply.set_result(false);
//                 quitguard.unlock();
//                 return;
//                 break;
//             }
//             default:
//                 break;
//             }

//         } else {
//             // cout << "empty" << endl;
//             // mtx.unlock();
//             // usleep(1000);
//         }
//     }
//     // should never goes here
//     cout << "not exiting properly";
//     quitguard.unlock();
//     return;
// }

void f(int64_t x, int64_t y) {
    for (int i = x; i <= y; i++) {
        if (skipList.search(i) != i + 1) {
            cout << "data corrupted" << i << " " << skipList.search(i) << endl;
            exit(0);
        }
    }
}
int main() {
    auto start = high_resolution_clock::now();
    for (int i = 1; i <= 20000000; i++) {
        skipList.insert(i, i + 1);
    }
    auto stop = high_resolution_clock::now();
    auto duration = duration_cast<milliseconds>(stop - start);

    // To get the value of duration use the count()
    // member function on the duration object
    printf("slap completed, time elapsed: %ldms, QPS: %lf/s\n",
           duration.count(),
           (double)20000000 / (double)duration.count() * 1000);

    cout << "init done" << endl;
    start = high_resolution_clock::now();

    cout << "validating data" << endl;
    for (int64_t i = 1; i <= 20000000; i++) {
        if (skipList.search(i) != i + 1) {
            cout << "data corrupted" << i << " " << skipList.search(i) << endl;
            exit(0);
        }
    }
    stop = high_resolution_clock::now();
    duration = duration_cast<milliseconds>(stop - start);

    // To get the value of duration use the count()
    // member function on the duration object
    printf("vaildate completed, time elapsed: %ldms, QPS: %lf/s\n",
           duration.count(),
           (double)20000000 / (double)duration.count() * 1000);
    cout << "data ok" << endl;
    start = high_resolution_clock::now();

    thread t1(f, 1, 20000000);
    thread t2(f, 1250001, 2500000);
    thread t3(f, 2500001, 3750000);
    thread t4(f, 3750001, 5000000);
    thread t5(f, 5000001, 6250000);
    thread t6(f, 6250001, 7500000);
    thread t7(f, 7500001, 8750000);
    thread t8(f, 8750001, 10000000);
    thread t9(f, 10000001, 11250000);
    thread t10(f, 11250001, 12500000);
    thread t11(f, 12500001, 13750000);
    thread t12(f, 13750001, 15000000);
    thread t13(f, 15000001, 16250000);
    thread t14(f, 16250001, 17500000);
    thread t15(f, 17500001, 18750000);
    thread t16(f, 18750001, 20000000);
    cout << "starting all threads" << endl;
    t1.join();
    t2.join();
    t3.join();
    t4.join();
    t5.join();
    t6.join();
    t7.join();
    t8.join();
    t9.join();
    t10.join();
    t11.join();
    t12.join();
    t13.join();
    t14.join();
    t15.join();
    t16.join();
    // thread w1(f);
    // w1.detach();
    // DBRequest req;
    // for (int64_t i = 1; i <= 20000000; i++) {
    //     // if (i % 1000000 == 0) {
    //     //     cout << i << endl;
    //     // }
    //     req.set_op(OP_INSERT);
    //     req.set_key1(i);
    //     req.set_key2(i + 1);
    //     // cout << "acquiring lock" << endl;
    //     // mtx.lock();
    //     // cout << "main locked" << endl;
    //     q.push(req);
    //     // mtx.unlock();
    // }
    // req.set_op(OP_ERROR);
    // req.set_key1(1);
    // req.set_key2(2);
    // // mtx.lock();
    // q.push(req);
    // // mtx.unlock();
    // quitguard.lock();
    stop = high_resolution_clock::now();
    duration = duration_cast<milliseconds>(stop - start);

    // To get the value of duration use the count()
    // member function on the duration object
    printf("slap completed, time elapsed: %ldms, QPS: %lf/s\n",
           duration.count(),
           (double)20000000 / (double)duration.count() * 1000);
    return 0;
}
