import pandas
from sqlalchemy import create_engine
import taosws

engine = taosws.connect("taos://localhost:6041")
res = pandas.read_sql("show databases", engine)
print(res)
