import sys
sys.path.append("../")
from DBServer import *
db = Conection('TEST')
db.test_conection()
print(db.Msg)