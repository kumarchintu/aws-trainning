#import os

fileName='PMT_Securities_20200425.csv'
if fileName.startswith('PMT_Positions'):
    print('File is position file')
elif fileName.startswith('PMT_Securities'):
    print('Securities file')
else:
    print('File not found')
