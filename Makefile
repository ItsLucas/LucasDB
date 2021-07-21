LD_FLAGS = -lev -lprotobuf -lpthread -ltcmalloc_minimal
CC_FLAGS = -O3 -Wall -I../include -pthread -ltcmalloc_minimal -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free
FLAGS += $(CC_FLAGS) $(LD_FLAGS)

all:
	g++ $(FLAGS) LucasDB.pb.cc skiplist.cpp -o skiplist 
	g++ $(FLAGS) LucasDB.pb.cc client.cpp -o client