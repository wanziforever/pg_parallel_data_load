parallel data loading tool for postgresql partition sharding architecture

if you are using the postgresql parition feature as the sharding feathre, and also use the FDW slice tables to external servers
the trandisional data loading method is to access the "master" machine, and send or copy data to the master, master will delivery
data to segment servers

parallel data loading tool will read your data and send the slice to the segments directly, will be many times of performance increase

delveloper: denny wang (wanziforever@163.com)