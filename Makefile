all: MFS
	gcc -pthread -o MFS MFS.c

clean:
	-rm -f MFS

run: MFS
	./MFS

LDFLAGS=-pthread
$(BIN): $(OBJ)
	g++ $(OBJ) -o $(BIN) $(LDFLAGS)