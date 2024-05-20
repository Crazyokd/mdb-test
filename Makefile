CXXFLAGS = -g -fpermissive

mdb-test: main.cpp util.cpp dbi.cpp
	bear -- g++ -DENABLE_ARRAY_TEST $^ -o $@ $(CXXFLAGS) $(shell pkg-config --libs --cflags libmongoc-1.0)

clean:
	-rm -rf *.o