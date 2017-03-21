from __future__ import print_function

import sys
import yarntf


print("hello")

yarntf.createClusterSpec()

print('Number of arguments:', len(sys.argv), 'arguments.')
print('Argument List:' + str(sys.argv))
