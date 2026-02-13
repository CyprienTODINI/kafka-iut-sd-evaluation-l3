from kafka import KafkaConsumer
import json
import uuid
import mysql.connector
import time

try:
    conn_cons = mysql.connector.connect(
        user="erpuser",
        password="ousYi423tT3HMcIc",
        host="mysql",
        port=3306,
        database="ermeurynome"
    )

    conn_prod = mysql.connector.connect(
        user="erpuser",
        password="ousYi423tT3HMcIc",
        host="mysql",
        port=3306,
        database="mygreaterp"
    )

except mysql.connector.Error as e:
    print(e)

try:
    cur_cons = conn_cons.cursor()
    cur_prod = conn_prod.cursor()

    # Requête SQL pour créer une table
    create_table_query = """
    CREATE TABLE IF NOT EXISTS contacts (
        contact_id VARCHAR(50) PRIMARY KEY,
        first_name VARCHAR(100) NOT NULL,
        last_name VARCHAR(100) NOT NULL,
        ig_glob VARCHAR(50) NOT NULL
    )

    CREATE TABLE IF NOT EXISTS contacts_adress (
        id VARCHAR(50) PRIMARY KEY,
        contact_id NOT NULL,
        adress_id NOT NULL
    )

    CREATE TABLE IF NOT EXISTS adress (
        adress_id VARCHAR(50) PRIMARY KEY,
        number VARCHAR(20) NOT NULL,
        street VARCHAR(255) NOT NULL, 
        city VARCHAR(255) NOT NULL,
        region VARCHAR(255) NOT NULL,
        ig_glob VARCHAR(50) NOT NULL
    )
    """
    cur_prod.execute(create_table_query)
    print("Table 'contact', 'adress', 'contacts_adress' créée avec succès.")

    cur_cons.commit()
    cur_prod.commit()
except mysql.connector.Error as e:
    print(f"Erreur : {e}")

# Define the Kafka broker and topic
broker = 'my-kafka.todini2u-dev.svc.cluster.local:9092'
topic = 'ERP'

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
    group_id='ERP'
)

print(f"Listening to topic {topic}")

# Poll messages from the Kafka topic
while True:
    for message in consumer:
        message_valeur = json.loads(message.value)
        print(message_valeur)
        valeur = message_valeur['payload']

        if valeur['before'] is None:
            id_glob = valeur['after']['id']

            id_contact = str(uuid.uuid4())
            first_name = valeur['after']['first_name']
            last_name = valeur['after']['last_name']

            id_adress = str(uuid.uuid4())
            number = valeur['after']['adresse_rue'].split(' ')[0]
            street = valeur['after']['adresse_rue'].split(' ')[1:]
            city = valeur['after']['adresse_ville']
            region = valeur['after']['adresse_region']

            cur_prod.execute(
                "INSERT INTO contacts VALUES (%s, %s, %s, %s)",
                (id_contact, first_name, last_name, id_glob)
            )

            cur_prod.execute(
                "INSERT INTO adress VALUES (%s, %s, %s, %s, %s, %s)",
                (id_adress, number, street, city, region, id_glob)
            )

            cur_prod.execute(
                "INSERT INTO contact_adress VALUES (%s, %s, %s)",
                (id_glob, id_contact, id_adress)
            )
        
        elif valeur['after'] is None:
            cur_prod.execute(
                "DELETE FROM contacts WHERE id_glob = (%s)", (id_glob)
            )

            cur_prod.execute(
                "DELETE FROM contacts_adress WHERE id_glob = (%s)", (id_glob)
            )

            cur_prod.execute(
                "DELETE FROM adress WHERE id_glob = (%s)", (id_glob)
            )
        
        elif valeur['after'] is not None and valeur['before'] is not None:
            id_glob = valeur['after']['id_glob']

            contact_id = valeur['after']['contact_id']
            first_name = valeur['after']['first_name']
            last_name = valeur['after']['last_name']

            adress_id = valeur['after']['adress_id']
            number = valeur['after']['adresse_rue'].split(' ')[0]
            street = valeur['after']['adresse_rue'].split(' ')[1:]
            city = valeur['after']['adresse_ville']
            region = valeur['after']['adresse_region']

            cur_prod.execute(
                "UPDATE contacts SET contact_id = %s, first_name = %s, last_name = %s , id_glob = %s WHERE id_glob = %s",
                (contact_id, first_name, last_name, id_glob, id_glob)
            )

            cur_prod.execute(
                "UPDATE adress SET adress_id = %s, number = %s, street = %s, city = %s, region = %s , id_glob = %s WHERE id_glob = %s",
                (adress_id, number, street, city, region, id_glob)
            )

        cur_prod.commit()
    time.sleep(0.5)