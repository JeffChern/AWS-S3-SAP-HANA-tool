import boto3
from datetime import datetime as d
import time as t
import json
import pyhdb

# Using pyhdb to connect with SAP HANA
connection = pyhdb.connect('localhost', 30015, 'YOUR_DB', 'YOUR_PASSWORD')
cursor = connection.cursor()
cursor.execute("SELECT MAX(uuid) FROM DEEPLENSDATA;")
max_num = cursor.fetchone()
uuid = max_num[0] +1
# Using boto3 to connect with AWS S3
s3 = boto3.resource('s3')
bucket = s3.Bucket('jeffdeeplens')

# Make sure we don't upload same data
cursor.execute("SELECT CONFIDENCE, DATA_TIME, NUMS FROM DEEPLENSDATA ORDER BY UUID DESC LIMIT 1")
tmp = cursor.fetchone()
conf = tmp[0]
time = tmp[1]
nums = tmp[2]
data = True;

while True:
  for obj in bucket.objects.all():
    body = obj.get()['Body'].read()
    jsons = json.loads(body)
    num = len(jsons)   
    # Inserting data into SAP HANA
    if num > 0:
      for j in jsons:
        if j['confidence'] == conf and d.strptime(j['timestamp'], '%Y-%m-%d %H:%M:%S.%f') == time and num == nums:
                print("We don't get new data")
                data = False     
        else:
          #print("not same")
          cursor.execute("INSERT INTO DEEPLENSDATA VALUES ({0}, '{1}', {2}, '{3}', {4});"
                      .format(uuid,j['object'],j['confidence'],j['timestamp'],num))
          uuid += 1
          data = True;
    else:
      cursor.execute("INSERT INTO DEEPLENSDATA VALUES ({0}, '{1}', {2}, '{3}', {4});"
                    .format(uuid,"face",0,d.now().strftime("%Y-%m-%d %H:%M:%S.%f"),0))
      uuid += 1
      data = True;

    if data:  
      if num > 1:
        print("We have {0} people here".format(num))
      else:
        print("We have {0} person here".format(num))
    
    t.sleep(1)
    connection.commit()
connection.close()
