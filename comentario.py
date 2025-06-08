import boto3
import uuid
import os
import json
from datetime import datetime

def lambda_handler(event, context):
    # Entrada (json)
    print(event)
    tenant_id = event['body']['tenant_id']
    texto = event['body']['texto']
    nombre_tabla = os.environ["TABLE_NAME"]
    nombre_bucket = os.environ["BUCKET_NAME"]
    
    # Proceso
    uuidv1 = str(uuid.uuid1())
    timestamp = datetime.now().isoformat()
    
    comentario = {
        'tenant_id': tenant_id,
        'uuid': uuidv1,
        'timestamp': timestamp,
        'detalle': {
          'texto': texto
        }
    }
    
    # Guardar en DynamoDB (como antes)
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(nombre_tabla)
    response_dynamo = table.put_item(Item=comentario)
    
    # NUEVO: Guardar en S3 bucket de ingesta según stage
    # Estrategia Push: El origen (lambda) envía los datos al destino (S3)
    try:
        s3_client = boto3.client('s3')
        
        # Crear estructura de carpetas por stage y tenant
        stage = os.environ.get('STAGE', 'dev')  # Obtener stage actual
        s3_key = f"{stage}/tenant_{tenant_id}/comentario_{uuidv1}.json"
        
        # Convertir comentario a JSON string
        comentario_json = json.dumps(comentario, indent=2, ensure_ascii=False)
        
        # Subir archivo JSON al bucket S3
        s3_response = s3_client.put_object(
            Bucket=nombre_bucket,
            Key=s3_key,
            Body=comentario_json,
            ContentType='application/json',
            Metadata={
                'tenant_id': tenant_id,
                'uuid': uuidv1,
                'stage': stage,
                'timestamp': timestamp
            }
        )
        
        print(f"Comentario guardado en S3: s3://{nombre_bucket}/{s3_key}")
        
    except Exception as e:
        print(f"Error al guardar en S3: {str(e)}")
        # No fallar la función si S3 falla, pero log el error
        s3_response = {"error": str(e)}
    
    # Salida (json)
    print(comentario)
    return {
        'statusCode': 200,
        'comentario': comentario,
        'dynamodb_response': response_dynamo,
        's3_response': s3_response,
        's3_location': f"s3://{nombre_bucket}/{stage}/tenant_{tenant_id}/comentario_{uuidv1}.json"
    }