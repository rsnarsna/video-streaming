import json
import boto3
import os
from datetime import datetime

dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
github = boto3.client("codepipeline", region_name="us-east-1")  # Alternative: use API call

DYNAMODB_TABLE = "video-uploads"
BUCKET = "video-streaming-quality-selection"

def lambda_handler(event, context):
    """
    S3 Event Handler:
    1. Extract videoId from path
    2. Update DynamoDB: status = processing
    3. Trigger GitHub Actions workflow
    4. Queue to SQS as backup
    """
    try:
        print("=== S3 EVENT TRIGGERED ===")
        
        records = event.get("Records", [])
        
        for record in records:
            bucket = record["s3"]["bucket"]["name"]
            key = record["s3"]["object"]["key"]
            size = record["s3"]["object"]["size"]
            
            print(f"S3 Key: {key}")
            
            # Extract videoId
            path_parts = key.split("/")
            if len(path_parts) < 4:
                continue
            
            video_id = path_parts[1]
            print(f"✓ Extracted videoId: {video_id}")
            
            # Update DynamoDB: Mark as processing
            update_video_status(video_id, "processing", size)
            
            # Trigger GitHub Actions
            trigger_github_actions(video_id, bucket, key)
            
            # Also queue to SQS as backup (in case GitHub Actions fails)
            queue_to_sqs(video_id, bucket, key, size)
        
        return {"statusCode": 200}
    
    except Exception as e:
        print(f"Error: {str(e)}")
        return {"statusCode": 500}


def update_video_status(video_id, status, file_size):
    """Update status in DynamoDB"""
    table = dynamodb.Table(DYNAMODB_TABLE)
    
    table.update_item(
        Key={"videoId": video_id},
        UpdateExpression="SET #status = :status, fileSize = :size, processingStarted = :time",
        ExpressionAttributeNames={"#status": "status"},
        ExpressionAttributeValues={
            ":status": status,
            ":size": file_size,
            ":time": datetime.utcnow().isoformat()
        }
    )
    
    print(f"✓ Updated {video_id} status to: {status}")


def trigger_github_actions(video_id, bucket, source_key):
    """
    Trigger GitHub Actions workflow
    
    Prerequisites:
    - GitHub repository with video-processing.yml workflow
    - GitHub Personal Access Token stored in AWS Secrets Manager
    """
    import requests
    
    try:
        # Get GitHub token from Secrets Manager
        secrets = boto3.client("secretsmanager", region_name="us-east-1")
        response = secrets.get_secret_value(SecretId="github-token")
        github_token = response["SecretString"]
        
        # GitHub API endpoint
        headers = {
            "Authorization": f"token {github_token}",
            "Accept": "application/vnd.github.v3+json"
        }
        
        # Trigger workflow
        url = "https://api.github.com/repos/YOUR_USERNAME/YOUR_REPO/actions/workflows/video-processing.yml/dispatches"
        
        payload = {
            "ref": "main",
            "inputs": {
                "videoId": video_id,
                "bucket": bucket,
                "sourceKey": source_key
            }
        }
        
        response = requests.post(url, json=payload, headers=headers)
        
        if response.status_code == 204:
            print(f"✓ GitHub Actions triggered for {video_id}")
        else:
            print(f"✗ Failed to trigger GitHub Actions: {response.text}")
    
    except Exception as e:
        print(f"Error triggering GitHub Actions: {str(e)}")
        raise


def queue_to_sqs(video_id, bucket, source_key, file_size):
    """Queue to SQS as backup"""
    sqs = boto3.client("sqs", region_name="us-east-1")
    queue_url = os.environ["PROCESSING_QUEUE_URL"]
    
    job = {
        "videoId": video_id,
        "bucket": bucket,
        "sourceKey": source_key,
        "fileSize": file_size,
        "timestamp": datetime.utcnow().isoformat(),
        "status": "queued"
    }
    
    sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(job)
    )
    
    print(f"✓ Job queued to SQS for {video_id}")
