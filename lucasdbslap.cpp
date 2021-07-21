#include <bits/stdc++.h>

using namespace std;

int main() {
    ofstream fout;
    fout.open("2kw.read");
    for (int i = 1; i <= 20000000; i++) {
        fout << "get"
             << " " << i << endl;
    }
    fout << "quit" << endl;
    fout.close();
    return 0;
}