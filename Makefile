LD_FLAGS = -lev -lprotobuf -lpthread
CC_FLAGS = -O2 -Wall -I../include -pthread  
FLAGS += $(CC_FLAGS) $(LD_FLAGS)

all:
	g++ $(FLAGS) LucasDB.pb.cc skiplist.cpp -o skiplist
	g++ $(FLAGS) LucasDB.pb.cc client.cpp -o client