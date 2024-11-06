from gold import comand_gold


def finalized_gold(event, context):
    bucket = event['bucket']
    key = event['name']
    
    if key.startswith('period'):
        comand_gold(bucket, key)