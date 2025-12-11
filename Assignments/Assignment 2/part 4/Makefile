.PHONY: build clean run-rr plot

build:
	@echo "Done"

# Run a single experiment with Round-Robin scheduling
run-rr:
	@echo "Running single Round-Robin experiment..."
	sudo env SHELL=/bin/bash python3 -c "from runner import Runner; r = Runner(); r.run_experiment(r.config['c'])"

# Run experiments for varying c values and generate the p4_plot.png
plot:
	@echo "Running experiments for Round-Robin scheduling..."
	sudo env SHELL=/bin/bash python3 runner.py

# Clean up all generated files
clean:
	@echo "Cleaning up generated files..."
	sudo rm -rf logs/ __pycache__/ *.pyc *.log results.csv experiment_results.json p4_plot.png
	sudo mn -c
	@echo "Cleanup complete."
	clear
