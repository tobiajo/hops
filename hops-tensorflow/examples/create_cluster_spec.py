from __future__ import print_function

import yarntf
from random import randint
print("hello")

rnd = randint(0, 65535)
yarntf.createClusterSpec('1337', rnd)
