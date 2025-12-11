# Compiler and flags
CXX = g++
CXXFLAGS = -std=c++17 -Wall

# Target executables
TARGET_SERVER = server
TARGET_CLIENT = client

# Python scripts
RUNNER = demo_runner.py
EXPERIMENT = run_experiments.py
PLOTTER = plot_results.py

# Phony targets
.PHONY: all build run plot clean

all: build

build: $(TARGET_SERVER) $(TARGET_CLIENT)

$(TARGET_SERVER): server.cpp
	$(CXX) $(CXXFLAGS) -o $(TARGET_SERVER) server.cpp

$(TARGET_CLIENT): client.cpp
	$(CXX) $(CXXFLAGS) -o $(TARGET_CLIENT) client.cpp

run: build
	# Single run with environment variables K and P
	sudo K=$${K:-5} P=$${P:-0} python3 $(RUNNER)

plot: build
	# Full experiment analysis and plotting
	sudo python3 $(EXPERIMENT)
	python3 $(PLOTTER)

clean:
	rm -f $(TARGET_SERVER) $(TARGET_CLIENT) results.csv p1_plot.png demo_config.json
	sudo rm -rf __pycache__/
	sudo mn -c
	clear