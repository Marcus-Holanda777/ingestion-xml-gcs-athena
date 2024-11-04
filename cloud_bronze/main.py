from bronze import comand_bronze


def finalized_bronze(event, context):
    bucket = event['bucket']
    key = event['name']
    
    if key.startswith('raw'):
        comand_bronze(bucket, key)

    