#!/usr/bin/python3
import datetime
import griddb_python
import random
import time
griddb = griddb_python
factory = griddb.StoreFactory.get_instance()

gridstore = factory.get_store(
    host="239.0.0.1",
    port=31999,
    cluster_name="defaultCluster",
    username="admin",
    password="admin"
)

gridstore.drop_container("tsepy")
expInfo = griddb.ExpirationInfo(1, griddb.TimeUnit.MINUTE, 5)

conInfo = griddb.ContainerInfo("tsepy",
    [["timestamp", griddb.Type.TIMESTAMP],
	["value", griddb.Type.FLOAT]],
	griddb.ContainerType.TIME_SERIES, expiration=expInfo)

ts = gridstore.put_container(conInfo)

i=0
while i < 20:
    ts.put([datetime.datetime.utcnow(), random.random()])

    q = ts.query("select * order by timestamp asc limit 1")
    rs = q.fetch(False)
    if rs.has_next():
        r = rs.next()
        print("Oldest is ",r[0])

    time.sleep(10)
    i=i+1
