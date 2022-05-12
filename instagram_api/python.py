import boto3

# Get the service resource.
dynamodb = boto3.resource('dynamodb')

# Create the DynamoDB table.
table = dynamodb.create_table(
    TableName='informacoes_conta_instagram',
    KeySchema=[
        {
            'AttributeName': 'Data_do_dia',
            'KeyType': 'HASH'
        },

    ],
    AttributeDefinitions=[
        {
            'AttributeName': 'Data_do_dia',
            'AttributeType': 'S'
        },

    ],
    ProvisionedThroughput={
        'ReadCapacityUnits': 5,
        'WriteCapacityUnits': 5
    }
)

# Wait until the table exists.
table.wait_until_exists()

# Print out some data about the table.
print(table.item_count)