CC      = g++
# Compiler Flags: Optimization, SIMD, Standard, and Includes
CFLAGS  = -O3 -mavx2 -std=c++14 -w -I./boost_1_84_0 -I/opt/homebrew/include

# Linker Flags: Library search paths
LDFLAGS = -L/opt/homebrew/lib


SOURCES = utils.cpp containers/relation.cpp containers/offsets_templates.cpp containers/offsets.cpp indices/1dgrid.cpp indices/hierarchicalindex.cpp indices/hint.cpp indices/hint_m.cpp indices/hint_m_delta.cpp indices/hint_m_subs+sort.cpp indices/hint_m_subs+sopt.cpp indices/hint_m_subs+sort+sopt.cpp indices/hint_m_subs+sort+sopt+ss.cpp indices/hint_m_subs+sort+cm.cpp indices/hint_m_subs+sort+sopt+cm.cpp indices/hint_m_subs+sort+ss+cm.cpp indices/hint_m_all.cpp
OBJECTS = $(SOURCES:.cpp=.o)

all: query

query: lscan 1dgrid hint hint_m hnit_m_delta
#check this 

lscan: $(OBJECTS)
	$(CC) $(CFLAGS) $(LDFLAGS) utils.o containers/relation.o main_lscan.cpp -o query_lscan.exec $(LDADD)

1dgrid: $(OBJECTS)
	$(CC) $(CFLAGS) $(LDFLAGS) utils.o containers/relation.o indices/1dgrid.o main_1dgrid.cpp -o query_1dgrid.exec $(LDADD)

hint: $(OBJECTS)
	$(CC) $(CFLAGS) $(LDFLAGS) utils.o containers/relation.o indices/hierarchicalindex.o indices/hint.o main_hint.cpp -o query_hint.exec $(LDADD)

hint_m: $(OBJECTS)
	$(CC) $(CFLAGS) $(LDFLAGS) utils.o containers/relation.o containers/offsets_templates.o containers/offsets.o indices/hierarchicalindex.o indices/hint_m.o indices/hint_m_subs+sort.o indices/hint_m_subs+sopt.o indices/hint_m_subs+sort+sopt.o indices/hint_m_subs+sort+sopt+ss.o indices/hint_m_subs+sort+sopt+cm.o indices/hint_m_subs+sort+cm.o indices/hint_m_subs+sort+ss+cm.o indices/hint_m_all.o main_hint_m.cpp -o query_hint_m.exec $(LDADD)

HINT_M_DELTA_OBJS = utils.o containers/relation.o containers/offsets_templates.o containers/offsets.o indices/hierarchicalindex.o indices/hint_m_simd.o indices/hint_m_delta.o

hint_m_delta: $(HINT_M_DELTA_OBJS)
	$(CC) $(CFLAGS) $(LDFLAGS) $(HINT_M_DELTA_OBJS) main_hint_m_delta.cpp -o query_hint_m_delta.exec $(LDADD)

query_hint_m_simd: $(OBJECTS) indices/hint_m_simd.o
	$(CC) $(CFLAGS) $(LDFLAGS) utils.o containers/relation.o containers/offsets_templates.o containers/offsets.o indices/hierarchicalindex.o indices/hint_m_simd.o indices/hint_m_subs+sort.o indices/hint_m_subs+sopt.o indices/hint_m_subs+sort+sopt.o indices/hint_m_subs+sort+sopt+ss.o indices/hint_m_subs+sort+sopt+cm.o indices/hint_m_subs+sort+cm.o indices/hint_m_subs+sort+ss+cm.o indices/hint_m_all.o main_hint_m.cpp -o query_hint_m_simd.exec $(LDADD)

duckdb_amalg/duckdb.o: duckdb_amalg/duckdb.cpp
	$(CC) $(CFLAGS) -c duckdb_amalg/duckdb.cpp -o duckdb_amalg/duckdb.o

duckdb_benchmark: $(HINT_M_DELTA_OBJS) duckdb_amalg/duckdb.o
	$(CC) $(CFLAGS) $(LDFLAGS) $(HINT_M_DELTA_OBJS) duckdb_amalg/duckdb.o duckdb_hint_benchmark.cpp -lpthread -ldl -o duckdb_hint_benchmark.exec $(LDADD)

duckdb_dynamic_benchmark: $(HINT_M_DELTA_OBJS) duckdb_amalg/duckdb.o
	$(CC) $(CFLAGS) $(LDFLAGS) $(HINT_M_DELTA_OBJS) duckdb_amalg/duckdb.o duckdb_dynamic_benchmark.cpp -lpthread -ldl -o duckdb_dynamic_benchmark.exec $(LDADD)


.cpp.o:
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -rf utils.o
	rm -rf containers/*.o
	rm -rf indices/*.o
	rm -rf duckdb_amalg/*.o
	rm -rf query_lscan.exec
	rm -rf query_1dgrid.exec
	rm -rf query_hint.exec
	rm -rf query_hint_m.exec
	rm -rf query_hint_m_delta.exec
	rm -rf query_hint_m_simd.exec
	rm -rf duckdb_hint_benchmark.exec
	rm -rf duckdb_dynamic_benchmark.exec
