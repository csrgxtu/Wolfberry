hadoop jar /home/archer/bin/hadoop-2.7.2/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar \
  -file ./map.py -mapper ./map.py \
  -file ./reduce.py -reducer ./reduce.py \
  -input /usr/archer/data/5000-8.txt -output /usr/archer/tmp/output
