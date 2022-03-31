import os
import sys

path = '/'

file_name = sys.argv[1]
for x,y,z in os.walk(path):
    # if len(z)!=0:
    for elem in z:
        if elem == file_name:
            print(x)
            sys.exit()
        # else:
        #     print('File not found')
        #     # sys.exit()
        #     break