.PHONY: build clean run-fcfs plot

build:
	@echo "Done"

# Run a single experiment with FCFS scheduling
run-fcfs:
	@echo "Running single FCFS experiment..."
	sudo env SHELL=/bin/bash python3 -c "from runner import Runner; r = Runner(); r.run_experiment(r.c)"

# Run experiments for varying c values and generate plot
plot:
	@echo "Running experiments with varying c values..."
	sudo env SHELL=/bin/bash python3 runner.py

# Clean up generated files
clean:
	@echo "Cleaning up files..."
	sudo rm -rf logs/
	sudo rm -rf __pycache__/
	sudo rm -f *.pyc
	sudo rm -f *.log
	sudo rm -f results.csv
	sudo rm -f p3_plot.png
	sudo mn -c
	@echo "Cleanup complete"
	clear