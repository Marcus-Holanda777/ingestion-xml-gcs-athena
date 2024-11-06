from silver import comand_silver


def finalized_silver(event, context):
    bucket = event['bucket']
    key = event['name']
    
    if key.startswith('lotes'):
        comand_silver(bucket, key)