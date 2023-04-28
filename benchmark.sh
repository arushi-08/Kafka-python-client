#!/bin/bash

# Wait for a few seconds to let the producer start sending messages
sleep 10

# Run the producer_benchmark.py script and print its output to the screen
python producer_benchmark.py | tee producer_output.txt &

# Wait for a few seconds to let the producer start sending messages
sleep 10

# Run the consumer_benchmark.py script and print its output to the screen
python consumer_benchmark.py | tee consumer_output.txt