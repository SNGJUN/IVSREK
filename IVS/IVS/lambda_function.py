import boto3
from decimal import Decimal
import json
import urllib.parse
from datetime import datetime

# AWS 서비스 클라이언트 초기화
rekognition = boto3.client('rekognition', region_name='ap-northeast-2')
ivs = boto3.client('ivs', region_name='ap-northeast-2')
dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-2')

# IVS 채널 ARN
channel_arn = 'arn:aws:ivs:ap-northeast-2:220563295011:channel/LofvRL0Ecbzt'

# DynamoDB 테이블 이름
dynamodb_table_name = 'ivsrek_dynamodb'

# 감지된 객체의 메타데이터 정의
PRODUCT_METADATA = {
    'Mobile Phone': {
        'type': 'ELECTRONICS',
        'title': 'SAMSUNG Z-FLIP4',
        'image': 'https://image-capture-hallym.s3.ap-northeast-2.amazonaws.com/product-images/z4_flip.png',        
        'description': 'MOBILE PHONE',
        'price': '400,000',
        'url': 'https://www.coupang.com/vp/products/7410157624?itemId=24279964131&vendorItemId=91402529449&q=%EA%B0%A4%EB%9F%AD%EC%8B%9C+z4+flip&itemsCount=36&searchId=2924196253a44787958f5b98b4a9129d&rank=6&searchRank=6&isAddedCart='
    },
    'Headphones': {
        'type': 'ELECTRONICS',
        'title': 'WH-1000XM5 SONY WIRELESS HEADPHONES',
        'image': 'https://image-capture-hallym.s3.ap-northeast-2.amazonaws.com/product-images/wh_100xm5.png',
        'description': 'HEADPHONES',
        'price': '300,000',
        'url': 'https://www.coupang.com/vp/products/6557157200?itemId=23516049552&vendorItemId=81904097274&q=wh1000xm5&itemsCount=36&searchId=bea6e6fe6f6445aeb0219858d7687f5b&rank=1&searchRank=1&isAddedCart='
    
    },
    'Mouse': {
        'type': 'ELECTRONICS',
        'title': 'LOGITECH LIFT MINI',
        'image': 'https://image-capture-hallym.s3.ap-northeast-2.amazonaws.com/product-images/logitech_lift.png',
        'description': 'COMPUTER MOUSE',
        'price': '50,000',
        'url': 'https://www.coupang.com/vp/products/6475321711?itemId=14150762010&vendorItemId=81397533118&q=%EB%A1%9C%EC%A7%80%ED%85%8D+lift+mini&itemsCount=36&searchId=de6b5f6ad1cb40d0b1ec8435049fbc0b&rank=1&searchRank=1&isAddedCart='
    },
    'Monitor': {
        'type': 'ELECTRONICS',
        'title': 'GALAXY TAB S8 ULTRA',
        'image': 'https://image-capture-hallym.s3.ap-northeast-2.amazonaws.com/product-images/s8_ultra.png',
        'description': 'SAMSUNG TABLET MONITOR',
        'price': '900,000',
        'url': 'https://www.coupang.com/vp/products/6473361880?itemId=22920849516&vendorItemId=90663813304&q=%EA%B0%A4%EB%9F%AD%EC%8B%9C%ED%83%AD+s8+%EC%9A%B8%ED%8A%B8%EB%9D%BC&itemsCount=36&searchId=3500cf88597d4a029848f813cbfa70f2&rank=0&isAddedCart='
    },
    'Laptop': {
        'type': 'ELECTRONICS',
        'title': 'LENOVO LAPTOP',
        'image': 'https://image-capture-hallym.s3.ap-northeast-2.amazonaws.com/product-images/lenovo_legion5.png',
        'description': 'LENOVO LEGION5 SLIM',
        'price': '1,200,000',
        'url': 'https://www.coupang.com/vp/products/7975828478?itemId=22892392171&vendorItemId=89926949341&q=lenovo+legion+slim+5&itemsCount=36&searchId=502a7f37748243ce995b09671db604e1&rank=28&searchRank=28&isAddedCart='
    }
}

# Rekognition API 호출 함수
def detect_labels(bucket, key):
    response = rekognition.detect_labels(Image={'S3Object': {'Bucket': bucket, 'Name': key}})
    print("Rekognition Labels:", json.dumps(response, indent=2))
    return response

# IVS 메타데이터 전송 함수
def put_metadata(channel_arn, metadata_payload):
    try:
        print(f"Attempting to send metadata: {metadata_payload}")
        response = ivs.put_metadata(channelArn=channel_arn, metadata=metadata_payload)
        print(f"put_metadata response: {response}")
        return response
    except Exception as e:
        print(f"Error sending metadata to IVS: {e}")
        raise e

# DynamoDB에 메타데이터 저장 함수
def save_metadata_to_dynamodb(metadata, bucket, key):
    table = dynamodb.Table(dynamodb_table_name)
    item = {
        'PK': key,
        'Bucket': bucket,
        'Metadata': metadata,
    }
    table.put_item(Item=item)
    print(f"Metadata saved to DynamoDB: {item}")

# Lambda 핸들러
def lambda_handler(event, context):
    print('Received event:', json.dumps(event, indent=2))

    # S3 버킷과 객체 키 가져오기
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
    
    try:
        # Rekognition을 사용하여 객체 레이블 감지
        rekognition_response = detect_labels(bucket, key)

        # 감지된 레이블 처리
        for label in rekognition_response['Labels']:
            label_name = label['Name']
            confidence = label['Confidence']

            # 감지된 레이블이 메타데이터 정의에 있는지 확인
            if label_name in PRODUCT_METADATA and confidence >= 90.0:
                print(f"Detected object: {label_name} (Confidence: {confidence:.2f})")
                
                # PRODUCT_METADATA dictionary의 label_name(key)을 확인 후 정보(value) 추출
                metadata = PRODUCT_METADATA[label_name]
                # meatadata에 confidence 레이블 추가
                metadata['confidence'] = f"{confidence:.2f}"
                # metadata 생성 로그 확인 
                print(f"Generated meatadata = {metadata}")

                # DynamoDB에 메타 데이터 저장
                try:
                    save_metadata_to_dynamodb(metadata, bucket, key)
                except Exception as e:
                    print(f"Error saving metadata to DynamoDB: {e}")

                # IVS에 메타데이터 전송
                metadata_payload = json.dumps(metadata)
                
                try:
                    print(f"Sending metadata to IVS: {metadata_payload}")
                    response = put_metadata(channel_arn, metadata_payload)
                    print("HTTPStatusCode: {}".format(response['ResponseMetadata']['HTTPStatusCode']))
                except Exception as e: 
                    print("ERROR: {}".format(e))
            else:
                print(f"Object '{label_name}' is not flagged for metadata transmission.")
        
        return {'status': 'success', 'message': 'Processing complete'}

    except Exception as e:
        print(f"Error processing object {key} from bucket {bucket}: {e}")
        raise e
