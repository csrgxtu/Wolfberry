hadoop jar ../lib/hadoop-streaming-2.5.0.jar \
  -file ./map.py -mapper ./map.py \
  -input /usr/archer/data/original_signal.json -output /usr/archer/tmp/output
