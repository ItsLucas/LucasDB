#include "bpt.h"
#include <bits/stdc++.h>
#include <chrono>
using namespace std::chrono;
using namespace bpt;

using namespace std;

int main() {
    auto start = high_resolution_clock::now();
    bplus_tree tree("lucasdb.idb", true);
    value_t v = -2;
    cout << tree.search(2, &v) << endl;
    cout << v << endl;
    tree.insert(1, 5);
    tree.insert(2, 6);
    tree.insert(3, 7);
    tree.remove(2);
    v = -2;
    cout << tree.search(2, &v) << endl;
    cout << v << endl;
    // for (int i = 1; i <= 1000000; i++) {
    //     if (i % 100000 == 0)
    //         cout << i << endl;
    //     value_t v;
    //     tree.search(i, &v);
    //     if (v != i + 1) {
    //         abort();
    //     }
    // }
    // auto stop = high_resolution_clock::now();
    // auto duration = duration_cast<milliseconds>(stop - start);

    // // To get the value of duration use the count()
    // // member function on the duration object
    // printf("slap completed, time elapsed: %ldms, QPS: %lf/s\n",
    //        duration.count(), (double)1000000 /
    //        (double)duration.count() * 1000);
    return 0;
}