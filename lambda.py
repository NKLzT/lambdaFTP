import os
import json
from ftplib import FTP
import boto3

#FTP_HOST = ftp.your_ftp_host.com
FTP_HOST = 'YOUR_FTP_HOST'
FTP_USER = 'YOUR_FTP_USER'
FTP_PWD = 'YOUR_FTP_PASSWORD'
#FTP_PATH = '/home/logs/'
FTP_PATH = 'YOUR_FTP_DESTINATION_PATH'


s3_client = boto3.client('s3')

def handler(event, context):

    if event and event['Records']:
        for record in event['Records']:
            sourcebucket = record['s3']['bucket']['name']
            sourcekey = record['s3']['object']['key']
            
            #Baixa os arquivos pra pasta temporaria
            filename = os.path.basename(sourcekey)
            download_path = '/tmp/'+ filename
            print(download_path)
            s3_client.download_file(sourcebucket, sourcekey, download_path)
            
            os.chdir("/tmp/")
            with FTP(FTP_HOST, FTP_USER, FTP_PWD) as ftp, open(filename, 'rb') as file:
                ftp.storbinary(f'STOR {FTP_PATH}{file.name}', file)

            #apagar os arquivos da pasta temporaria
            os.remove(filename)