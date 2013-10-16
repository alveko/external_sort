CXX	?= g++

CFLAGS	= -std=c++11 -c -Wall -pthread -DBOOST_LOG_DYN_LINK
INCL	= -I. -I./logging -I/usr/local/include
LDFLAGS	= \
	-pthread \
	-L/usr/local/lib \
	-lboost_program_options \
	-lboost_timer \
	-lboost_system \
	-lboost_log \
	-lboost_log_setup \
	-lboost_thread

EXE	= external_sort
SRC	= external_sort.cc logging/logging.cc

OBJ	= $(SRC:.cc=.o)

.PHONY: all clean

all:	CFLAGS += -O3
all:	$(EXE)

debug:	CFLAGS += -g -DDEBUG -DBOOSTLOG
debug:	$(EXE)

$(EXE): $(OBJ)
	$(CXX) $(OBJ) -o $@ $(LDFLAGS)

.cc.o:
	$(CXX) $(CFLAGS) $(INCL) $< -o $@

clean:
	rm -f $(EXE) $(OBJ)
