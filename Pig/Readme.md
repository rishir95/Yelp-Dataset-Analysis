

The following file needs to be downloaded:
1.json-simple-1.1.1.jar;
2.elephant-bird-hadoop-compat-4.3.jar;
3.elephant-bird-pig-4.3.jar;

----------------------------
A. Executing .pig file
----------------------------
```
[root@quickstart /]# pig -x local
grunt> exec /path/filename.pig;
```
Each .pig file registers jars and stores the result at a predefined directory.