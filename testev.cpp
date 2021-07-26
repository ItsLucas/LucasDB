#include "LucasDB.pb.h"
#include <boost/random.hpp>
#include <evpp/buffer.h>
#include <evpp/event_loop_thread_pool.h>
#include <evpp/tcp_client.h>
#include <evpp/tcp_conn.h>
#include <iostream>

using namespace std;
int ri, ru, rd, rg;
double probabilities[] = {0.5, 0.2, 0.2, 0.1};
boost::random::taus88 keygen;
boost::random::discrete_distribution<int> dist(probabilities);

int main() {
    for (int i = 1; i <= 10; i++) {
        cout << dist(keygen) << endl;
    }
    return 0;
}