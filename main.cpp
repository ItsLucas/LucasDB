#include "skiplist.h"
#include <cstdlib>
#include <iostream>
#include <list>
#include <thread>

using namespace std;

using namespace SL;

SkipList<int64_t, uint32_t> skipList(25);
inline void f(unsigned int seed) {
    srand(seed);
    for (int64_t i = 1; i <= 20000000; i++) {
        // if (i % 375000 == 0) {
        //     cout << i << endl;
        // }
        skipList.insert(i, rand());
    }
}
int main() {
    // thread t1(f, 1, 1250000);
    // thread t2(f, 1250001, 2500000);
    // thread t3(f, 2500001, 3750000);
    // thread t4(f, 3750001, 5000000);
    // thread t5(f, 5000001, 6250000);
    // thread t6(f, 6250001, 7500000);
    // thread t7(f, 7500001, 8750000);
    // thread t8(f, 8750001, 10000000);
    // cout << "starting all threads" << endl;
    // t1.join();
    // t2.join();
    // t3.join();
    // t4.join();
    // t5.join();
    // t6.join();
    // t7.join();
    // t8.join();
    // cout << "validating data" << endl;
    // for (int64_t i = 1; i <= 10000000; i++) {
    //     if (skipList.search(i) != i + 1) {
    //         cout << "data corrupted" << i << " " << skipList.search(i) <<
    //         endl; exit(0);
    //     }
    // }
    // cout << "data ok" << endl;
    int seed = 1202 + 15565;
    f(seed);
    return 0;
}
