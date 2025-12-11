build:
	@echo "Done"

# Runs a single experiment with num_clients from config.json
run:
	sudo env SHELL=/bin/bash python3 -c "import json; from run_part2_analysis import Part2Runner; f=open('config.json'); config=json.load(f); nc=config.get('num_clients', 1); r=Part2Runner(); r.run_single_experiment(nc)"

# Runs the full analysis for varying numbers of clients
plot:
	sudo env SHELL=/bin/bash python3 run_part2_analysis.py
	python3 plot_part2.py

clean:
	sudo mn -c
	rm -f part2_results.csv p2_plot.png
	sudo rm -rf __pycache__/
	clear

.PHONY: build run plot clean