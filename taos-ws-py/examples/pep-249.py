import taosws
conn = taosws.connect("ws://localhost:6041")

cursor = conn.cursor()

cursor.execute("show databases")
results = cursor.fetch_all()
for row in results:
    print(row)