from kafka import KafkaConsumer
import json
import uuid
import mysql.connector
import time

try:
    conn = mysql.connector.connect(
        user="crmuser",
        password="ousYi423tT3HMcIc",
        host="mysql",
        port=3306,
        database="ermeurynome"
    )

except mysql.connector.Error as e:
    print(e)

try:
    cur = conn.cursor()

    # Requête SQL pour créer une table
    create_table_query = """
    CREATE TABLE IF NOT EXISTS contacts (
        id VARCHAR(50) PRIMARY KEY,
        first_name VARCHAR(100) NOT NULL,
        last_name VARCHAR(100) NOT NULL
    )
    """
    cur.execute(create_table_query)
    print("Table 'contact' créée avec succès.")

    conn.commit()
except mysql.connector.Error as e:
    print(f"Erreur : {e}")

# Define the Kafka broker and topic
broker = 'my-kafka.todini2u-dev.svc.cluster.local:9092'
topic = 'contacts'

# Create a Kafka consumer
consumer = KafkaConsumer(
    topic,
    bootstrap_servers=[broker],
    sasl_mechanism='SCRAM-SHA-256',
    security_protocol='SASL_PLAINTEXT',
    sasl_plain_username='user1',
    sasl_plain_password='uaiSSxIRyx',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='contacts'
)

print(f"Listening to topic {topic}")

# Poll messages from the Kafka topic
while True:
    for message in consumer:
        message_valeur = json.loads(message.value)
        print(message_valeur)
        valeur = message_valeur['payload']
        id = str(uuid.uuid4())

        if valeur['before'] is None:
            id_glob = valeur['after']['id']
            first_name = valeur['after']['first_name']
            last_name = valeur['after']['last_name']
            cur.execute(
                "INSERT INTO contacts VALUES (%s, %s, %s, %s)",
                (id, first_name, last_name, id_glob)
            )
        
        elif valeur['after'] is None:
            cur.execute(
                "DELETE FROM contacts WHERE id = (%s)", (id_glob)
            )
        
        elif valeur['after'] is not None and valeur['before'] is not None:
            id = valeur['after']['id']
            first_name = valeur['after']['first_name']
            last_name = valeur['after']['last_name']

            cur.execute(
                "UPDATE contacts SET id = %s, first_name = %s, last_name = %s , id = %s WHERE id = %s",
                (id, first_name, last_name, id_glob ,id)
            )

        conn.commit()
    time.sleep(0.5)