#define OP_INSERT 0
#define OP_UPDATE 1
#define OP_DELETE 2
#define OP_GET 3
#define OP_RANGE 4
#define OP_MUL2 5
#define OP_RANDINIT 6
#define OP_ERROR 255

#include <string>

static std::string dict[5] = {"insert", "update", "delete", "get", "range"};