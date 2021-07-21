#include <cstdlib>
#include <iostream>

using namespace std;

int main() {
    int x;
    cin >> x;
    srand(x);
    for (int i = 1; i <= 10; i++) {
        cout << rand() << " ";
    }
    cout << endl;
}