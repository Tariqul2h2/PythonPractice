from importlib.resources import path
import os
path = '/home/tariq/Desktop'
print(list(os.walk(path)))
# for dir,folds,files in os.walk(path):
#     print(files)

