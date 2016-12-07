import json
import re

from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol

WORD_RE = re.compile(r"[A-Za-z]+")


class MRNickNack(MRJob):

    # HADOOP_OUTPUT_FORMAT = 'nicknack.MultipleValueOutputFormat'
    #
    # LIBJARS = ['nicknack-1.0.0.jar']

    OUTPUT_PROTOCOL = RawValueProtocol

    def mapper(self, _, line):
        for word in WORD_RE.findall(line):
            yield (word.lower(), 1)

    def reducer(self, word, counts):
        total = sum(counts)
        # yield None, '\t'.join([word[0], json.dumps(word), json.dumps(total)])
        yield None, json.dumps(total)


if __name__ == '__main__':
    MRNickNack.run()
