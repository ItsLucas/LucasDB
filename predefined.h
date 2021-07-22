#ifndef PREDEFINED_H
#define PREDEFINED_H

#include <string.h>
namespace bpt {

/* predefined B+ info */
#define BP_ORDER 20

/* key/value type */
typedef u_int32_t value_t;
typedef int64_t key_t;

inline int keycmp(const key_t &a, const key_t &b) {
    if (a == b)
        return 0;
    else
        return a > b ? 1 : -1;
}

#define OPERATOR_KEYCMP(type)                                                  \
    bool operator<(const key_t &l, const type &r) {                            \
        return keycmp(l, r.key) < 0;                                           \
    }                                                                          \
    bool operator<(const type &l, const key_t &r) {                            \
        return keycmp(l.key, r) < 0;                                           \
    }                                                                          \
    bool operator==(const key_t &l, const type &r) {                           \
        return keycmp(l, r.key) == 0;                                          \
    }                                                                          \
    bool operator==(const type &l, const key_t &r) {                           \
        return keycmp(l.key, r) == 0;                                          \
    }

} // namespace bpt

#endif /* end of PREDEFINED_H */
