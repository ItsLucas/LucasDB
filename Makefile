LD_FLAGS = -lpthread -levent -lprotobuf -lglog -levpp -ltcmalloc_minimal -lprofiler 
CC_FLAGS = -O3 -Wall -I../include -pthread -ltcmalloc_minimal -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free
FLAGS += $(CC_FLAGS) $(LD_FLAGS)

all:
	g++ $(FLAGS) -lev bpt.cc LucasDB.pb.cc skiplist.cpp -o skiplist 
	g++ $(FLAGS) -lev LucasDB.pb.cc client.cpp -o client
multi:
	g++ $(FLAGS) bpt.cc LucasDB.pb.cc skiplist-multi.cpp -o skiplist_multi 
	g++ $(FLAGS) LucasDB.pb.cc client-multi.cpp -o client_multi