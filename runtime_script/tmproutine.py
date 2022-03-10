import os
import glob

files = glob.glob('../data/tmp/data/tmp_pdf_*')
for f in files:
    os.remove(f)
