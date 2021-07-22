#include "node.h"
#include <chrono>
#include <cstring>
#include <iostream>
#include <list>
using namespace std::chrono;
using namespace std;

list<int> skipList;
int main() {
    auto start = high_resolution_clock::now();
    for (int i = 1; i <= 20000000; i++) {
        skipList.emplace_back(i);
    }
    auto stop = high_resolution_clock::now();
    auto duration = duration_cast<milliseconds>(stop - start);

    // To get the value of duration use the count()
    // member function on the duration object
    printf("slap completed, time elapsed: %ldms, QPS: %lf/s\n",
           duration.count(),
           (double)20000000 / (double)duration.count() * 1000);

    return 0;
}