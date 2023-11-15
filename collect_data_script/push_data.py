from minio import Minio
import os

source_ts = os.popen("TZ=\"EST\" date +\"%d-%m-%y %r\"").read()
source_ts = source_ts.strip()
source_memory = os.popen("free -m | awk \'NR==2{printf \"%.2f%%\t\t\", $3*100/$2 }\'").read()
source_memory = source_memory.strip()
source_disk = os.popen("df -h | awk \'$NF==\"/\"{printf \"%s\t\t\", $5}\'").read()
source_disk = source_disk.strip()
source_cpu = os.popen("top -bn1 | grep load | awk \'{printf \"%.2f%%\t\t\", $(NF-2) }\'").read()
source_cpu = source_cpu.strip()
source_hostname = os.popen("hostname").read()
source_hostname = source_hostname.strip()
name_source_ts = os.popen("TZ=\"EST\" date +\"%d%m%y%H%M%S\"").read()
name_source_ts = name_source_ts.strip()
print(name_source_ts)
f = open("vm" + name_source_ts + ".log", "w")
f.write("timestamp: " + source_ts + ", used_memory: " + source_memory + \
        ", used_storage: " + source_disk + ", used_cpu: " + source_cpu + ", hostname: " +source_hostname)
f.close()

LOCAL_FILE_PATH = "vm" + name_source_ts + ".log"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
MINIO_API_HOST = "http://localhost:9000"
MINIO_CLIENT = Minio("localhost:9000", access_key=ACCESS_KEY, secret_key=SECRET_KEY, secure=False)

bucketname = 'external-acumos-vm-data'
found = MINIO_CLIENT.bucket_exists(bucketname)
if not found:
   MINIO_CLIENT.make_bucket(bucketname)
else:
   print(f"Bucket {bucketname} already exists")
MINIO_CLIENT.fput_object(bucketname, LOCAL_FILE_PATH, LOCAL_FILE_PATH)
print(f"File {LOCAL_FILE_PATH} is successfully uploaded to bucket")
