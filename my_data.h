enum SQL_CMD { SQL_INSERT, SQL_UPDATE, SQL_DELETE, SQL_GET, SQL_RANGE };

struct my_data {
    enum SQL_CMD cmd;
    long long k1;
    unsigned int k2;
    unsigned int val;
    int ret;
};