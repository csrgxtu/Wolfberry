hadoop jar /home/archer/bin/hadoop-2.7.2/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar \
  -file ./map.py -mapper ./map.py \
  -input /usr/archer/data/original_signal.json -output /usr/archer/tmp/output
