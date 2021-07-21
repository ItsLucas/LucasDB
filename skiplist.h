#ifndef SKIPLIST_H
#define SKIPLIST_H
#include "node.h"
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <mutex>
#include <vector>
#define STORE_FILE "mdump"
using namespace std;
namespace SL {

template <typename K, typename V> class SkipList {
  public:
    SkipList(int);
    ~SkipList();
    int toll();
    Node<K, V> *alloc(K, V, int);
    bool insert(K, V);
    void print();
    int search(K);
    bool remove(K);
    bool update(K, V);
    void unmap();
    void map();
    int size();
    vector<V> range(K, K);

  private:
    void _format(const std::string &str, std::string *key, std::string *value);
    bool _check(const std::string &str);

  private:
    int max_level;

    int current_level;

    Node<K, V> *head;

    std::ofstream out;
    std::ifstream in;

    int count;
    std::mutex mtx;
    std::string delimiter = ":";
};

template <typename K, typename V>
Node<K, V> *SkipList<K, V>::alloc(const K k, const V v, int level) {
    Node<K, V> *n = new Node<K, V>(k, v, level);
    return n;
}

template <typename K, typename V>
bool SkipList<K, V>::insert(const K key, const V value) {
    // cout << "Inserting" << key << " " << value << endl;
    mtx.lock();
    Node<K, V> *current = this->head;
    Node<K, V> *update[max_level + 1];
    memset(update, 0, sizeof(Node<K, V> *) * (max_level + 1));

    for (int i = current_level; i >= 0; i--) {
        while (current->forward[i] != NULL &&
               current->forward[i]->get_key() < key) {
            current = current->forward[i];
        }
        update[i] = current;
    }

    current = current->forward[0];

    if (current != NULL && current->get_key() == key) {
        mtx.unlock();
        return false;
    }

    if (current == NULL || current->get_key() != key) {
        int random_level = toll();

        if (random_level > current_level) {
            for (int i = current_level + 1; i < random_level + 1; i++) {
                update[i] = head;
            }
            current_level = random_level;
        }

        Node<K, V> *inserted_node = alloc(key, value, random_level);

        for (int i = 0; i <= random_level; i++) {
            inserted_node->forward[i] = update[i]->forward[i];
            update[i]->forward[i] = inserted_node;
        }
        count++;
    }
    mtx.unlock();
    return true;
}

template <typename K, typename V> void SkipList<K, V>::print() {
    // for (int i = 0; i <= current_level; i++) {
    //   Node<K, V> *node = this->head->forward[i];
    //   std::cout << "Level " << i << ": ";
    //   while (node != NULL) {
    //     std::cout << node->get_key() << ":" << node->get_value() << ";";
    //     node = node->forward[i];
    //   }
    //   std::cout << std::endl;
    // }
}

template <typename K, typename V> void SkipList<K, V>::unmap() {
    out.open(STORE_FILE);
    Node<K, V> *node = this->head->forward[0];

    while (node != NULL) {
        out << node->get_key() << ":" << node->get_value() << "\n";
        node = node->forward[0];
    }

    out.flush();
    out.close();
    return;
}

template <typename K, typename V> void SkipList<K, V>::map() {
    in.open(STORE_FILE);
    std::string line;
    std::string *key = new std::string();
    std::string *value = new std::string();
    while (getline(in, line)) {
        _format(line, key, value);
        if (key->empty() || value->empty()) {
            continue;
        }
        insert(*key, *value);
    }
    in.close();
}

template <typename K, typename V> int SkipList<K, V>::size() { return count; }

template <typename K, typename V>
void SkipList<K, V>::_format(const std::string &str, std::string *key,
                             std::string *value) {
    if (!_check(str)) {
        return;
    }
    *key = str.substr(0, str.find(delimiter));
    *value = str.substr(str.find(delimiter) + 1, str.length());
}

template <typename K, typename V>
bool SkipList<K, V>::_check(const std::string &str) {
    if (str.empty()) {
        return false;
    }
    if (str.find(delimiter) == std::string::npos) {
        return false;
    }
    return true;
}

template <typename K, typename V> bool SkipList<K, V>::remove(K key) {
    bool stat = false;
    mtx.lock();
    Node<K, V> *current = this->head;
    Node<K, V> *update[max_level + 1];
    memset(update, 0, sizeof(Node<K, V> *) * (max_level + 1));

    for (int i = current_level; i >= 0; i--) {
        while (current->forward[i] != NULL &&
               current->forward[i]->get_key() < key) {
            current = current->forward[i];
        }
        update[i] = current;
    }

    current = current->forward[0];
    if (current != NULL && current->get_key() == key) {
        stat = true;
        for (int i = 0; i <= current_level; i++) {
            if (update[i]->forward[i] != current)
                break;

            update[i]->forward[i] = current->forward[i];
        }

        while (current_level > 0 && head->forward[current_level] == 0) {
            current_level--;
        }

        count--;
    }
    mtx.unlock();
    return stat;
}

template <typename K, typename V> int SkipList<K, V>::search(K key) {
    Node<K, V> *current = head;

    for (int i = current_level; i >= 0; i--) {
        while (current->forward[i] && current->forward[i]->get_key() < key) {
            current = current->forward[i];
        }
    }

    current = current->forward[0];

    if (current and current->get_key() == key) {
        // printf("Found value %u in %ld\n", current->get_value(), key);
        return current->get_value();
    }
    return -1;
}

template <typename K, typename V> vector<V> SkipList<K, V>::range(K l, K r) {
    SkipList<K, V>::print();
    vector<V> v;
    Node<K, V> *current = head;
    for (int i = current_level; i >= 0; i--) {
        while (current->forward[i] && current->forward[i]->get_key() < l) {
            current = current->forward[i];
        }
    }

    current = current->forward[0];

    while (current and current->get_key() >= l and current->get_key() <= r) {
        // cout<<"Accessing "<<current->get_key()<<",
        // "<<current->get_value()<<endl;
        v.push_back(current->get_value());
        current = current->forward[0];
    }
    return v;
}

template <typename K, typename V> bool SkipList<K, V>::update(K key, V value) {
    Node<K, V> *current = head;

    for (int i = current_level; i >= 0; i--) {
        while (current->forward[i] && current->forward[i]->get_key() < key) {
            current = current->forward[i];
        }
    }

    current = current->forward[0];

    if (current and current->get_key() == key) {
        current->set_value(value);
        return true;
    }
    return false;
}

template <typename K, typename V> SkipList<K, V>::SkipList(int max_level) {
    this->max_level = max_level;
    this->current_level = 0;
    this->count = 0;
    K k;
    V v;
    this->head = new Node<K, V>(k, v, max_level);
};

template <typename K, typename V> SkipList<K, V>::~SkipList() {
    if (out.is_open()) {
        out.close();
    }
    if (in.is_open()) {
        in.close();
    }
    delete head;
}

template <typename K, typename V> int SkipList<K, V>::toll() {
    int k = 1;
    while (rand() % 2) {
        k++;
    }
    k = (k < max_level) ? k : max_level;
    return k;
};
} // namespace SL
#endif
