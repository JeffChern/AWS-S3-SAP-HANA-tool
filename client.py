import boto3
import time
import json
import pyhdb
import datetime
# Using pyhdb to connect with SAP HANA
connection = pyhdb.connect('localhost', 30015, 'YOUR_USERNAME', 'YOUR_PASSWORD')
cursor = connection.cursor()
cursor.execute("SELECT MAX(uuid) FROM DEEPLENSDATA;")
max_num = cursor.fetchone()
uuid = max_num[0] +1
# Using boto3 to connect with AWS S3
s3 = boto3.resource('s3')
bucket = s3.Bucket('jeffdeeplens')

while True:
  for obj in bucket.objects.all():
    body = obj.get()['Body'].read()
    jsons = json.loads(body)
    num = len(jsons)
    # Inserting data into SAP HANA
    if num > 0:
      for j in jsons:
        cursor.execute("INSERT INTO DEEPLENSDATA VALUES ({0}, '{1}', {2}, '{3}', {4});"
                       .format(uuid,j['object'],j['confidence'],j['timestamp'],num))
        uuid += 1
    else:
      cursor.execute("INSERT INTO DEEPLENSDATA VALUES ({0}, '{1}', {2}, '{3}', {4});"
                     .format(uuid,"face",0,datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),0))
      uuid += 1

    if num > 1:
      print("We have {0} people here".format(num))
    else:
      print("We have {0} person here".format(num))
    time.sleep(1)
    connection.commit()
    
connection.close()
