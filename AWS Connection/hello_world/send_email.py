import boto3
from botocore.exceptions import ClientError
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

SENDER = os.environ['SENDER']       # comes from template.yaml
RECIPIENT = os.environ['RECIPIENT'] # comes from template.yaml

ses_client = boto3.client('ses')

def send_email(attachment_bytes=None, attachment_name=None):  
    msg = MIMEMultipart()
    msg['Subject'] = "Shelter Connect: Potential Matches"
    msg['From'] = SENDER
    msg['To'] = RECIPIENT

    # add body
    msg.attach(MIMEText("Your potential shelter matches are attached to this email.", 'plain'))

    # Attach the Excel file
    if attachment_bytes and attachment_name:
        part = MIMEApplication(attachment_bytes, Name=attachment_name)
        part["Content-Disposition"] = f'attachment; filename="{attachment_name}"'
        msg.attach(part)

    try:
        response = ses_client.send_raw_email(
            Source=SENDER,
            Destinations=[RECIPIENT],
            RawMessage={'Data': msg.as_bytes()}
        )
        return {
            'statusCode': 200,
            'body': f"Email sent! Message ID: {response['MessageId']}"
        }
    except ClientError as e:
        print(e.response['Error']['Message'])
        return {
            'statusCode': 500,
            'body': e.response['Error']['Message']
        }
