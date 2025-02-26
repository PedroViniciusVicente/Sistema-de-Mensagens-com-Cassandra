from cassandra.cluster import Cluster

import uuid                                 # Lib que gera os IDs
from datetime import datetime, timedelta    # Lib que pega os tempos


session = None  # Variável global

def inicializar_database():

    ############### CONEXAO CLUSTER DO CASSANDRA ###############

    global session
    cluster = Cluster(['127.0.0.1'])  # (IP local)
    # cluster = Cluster(['127.0.0.1'], port=9042) # Esse já vem com o port
    session = cluster.connect()



    ############### CRIAÇÃO E USO DO KEYSPACE ###############

    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS trabalho3keyspace
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
    """)

    session.set_keyspace('trabalho3keyspace') # Usar o keyspace



    ############### CRIAÇÃO DAS TABLES ###############
    # drops para zerar as alteracoes
    session.execute("DROP MATERIALIZED VIEW IF EXISTS mensagens_por_usuario_tema;")
    session.execute("DROP MATERIALIZED VIEW IF EXISTS mensagens_por_id;")
    session.execute(""" DROP TABLE IF EXISTS mensagens; """)


    session.execute("""
        CREATE TABLE IF NOT EXISTS mensagens (
            usuario_id UUID,
            data_postagem TIMESTAMP,
            mensagem_id UUID,
            idade_usuario INT,
            tema TEXT,
            texto TEXT,
            PRIMARY KEY ((usuario_id), data_postagem, mensagem_id)
        ) WITH CLUSTERING ORDER BY (data_postagem DESC);
    """)
    # Observação:
    # - usuario_id eh a partition key
    # - data_postagem e mensagem_id são as clustering keys (definem ordenacao dentro da partition)



    ############### INSERÇÃO DE 20 MENSAGENS ###############

    usuario1 = uuid.uuid4()
    usuario2 = uuid.uuid4()
    usuario3 = uuid.uuid4()
    usuario4 = uuid.uuid4()
    usuario5 = uuid.uuid4()


    def insert_mensagem(usuario_id, mensagem_id, data_postagem, idade_usuario, tema, texto):
        session.execute("""
            INSERT INTO mensagens (usuario_id, data_postagem, mensagem_id, idade_usuario, tema, texto)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (usuario_id, data_postagem, mensagem_id, idade_usuario, tema, texto))

    insert_mensagem(usuario1, uuid.uuid4(), datetime.now(), 25, "Esportes", "Mensagem de exemplo número 1 sobre Esportes.")
    insert_mensagem(usuario3, uuid.uuid4(), datetime.now() - timedelta(days=2), 22, "Música", "Mensagem de exemplo número 3 sobre Música.")
    insert_mensagem(usuario2, uuid.uuid4(), datetime.now() - timedelta(days=1), 30, "Tecnologia", "Mensagem de exemplo número 2 sobre Tecnologia.")
    insert_mensagem(usuario1, uuid.uuid4(), datetime.now() - timedelta(days=3), 28, "Cinema", "Mensagem de exemplo número 4 sobre Cinema.")
    insert_mensagem(usuario2, uuid.uuid4(), datetime.now() - timedelta(days=4), 35, "Ciência", "Mensagem de exemplo número 5 sobre Ciência.")
    insert_mensagem(usuario4, uuid.uuid4(), datetime.now() - timedelta(days=5), 40, "Esportes", "Mensagem de exemplo número 6 sobre Esportes.")
    insert_mensagem(usuario4, uuid.uuid4(), datetime.now() - timedelta(days=6), 26, "Tecnologia", "Mensagem de exemplo número 7 sobre Tecnologia.")
    insert_mensagem(usuario3, uuid.uuid4(), datetime.now() - timedelta(days=7), 33, "Música", "Mensagem de exemplo número 8 sobre Música.")
    insert_mensagem(usuario4, uuid.uuid4(), datetime.now() - timedelta(days=8), 21, "Cinema", "Mensagem de exemplo número 9 sobre Cinema.")
    insert_mensagem(usuario1, uuid.uuid4(), datetime.now() - timedelta(days=9), 29, "Ciência", "Mensagem de exemplo número 10 sobre Ciência.")
    insert_mensagem(usuario2, uuid.uuid4(), datetime.now() - timedelta(days=10), 37, "Esportes", "Mensagem de exemplo número 11 sobre Esportes.")
    insert_mensagem(usuario3, uuid.uuid4(), datetime.now() - timedelta(days=11), 24, "Tecnologia", "Mensagem de exemplo número 12 sobre Tecnologia.")
    insert_mensagem(usuario5, uuid.uuid4(), datetime.now() - timedelta(days=12), 31, "Música", "Mensagem de exemplo número 13 sobre Música.")
    insert_mensagem(usuario1, uuid.uuid4(), datetime.now() - timedelta(days=13), 23, "Cinema", "Mensagem de exemplo número 14 sobre Cinema.")
    insert_mensagem(usuario2, uuid.uuid4(), datetime.now() - timedelta(days=14), 27, "Ciência", "Mensagem de exemplo número 15 sobre Ciência.")
    insert_mensagem(usuario5, uuid.uuid4(), datetime.now() - timedelta(days=15), 36, "Esportes", "Mensagem de exemplo número 16 sobre Esportes.")
    insert_mensagem(usuario5, uuid.uuid4(), datetime.now() - timedelta(days=16), 20, "Tecnologia", "Mensagem de exemplo número 17 sobre Tecnologia.")
    insert_mensagem(usuario3, uuid.uuid4(), datetime.now() - timedelta(days=17), 32, "Música", "Mensagem de exemplo número 18 sobre Música.")
    insert_mensagem(usuario1, uuid.uuid4(), datetime.now() - timedelta(days=18), 34, "Cinema", "Mensagem de exemplo número 19 sobre Cinema.")
    insert_mensagem(usuario2, uuid.uuid4(), datetime.now() - timedelta(days=19), 38, "Ciência", "Mensagem de exemplo número 20 sobre Ciência.")




    ############### CRIAÇÃO DOS INDICES ###############

    # Indice para agilizar as consultas por mensagem_id
    session.execute("""CREATE INDEX idx_mensagem_id ON mensagens (mensagem_id);""")


    # Criar um índice e executar a consulta: “recuperar, para cada usuário, os tipos de mensagens enviadas e a frequência delas”.
    session.execute("""CREATE INDEX idx_tema ON mensagens (tema);""")

    # Criar um índice para consultas sobre data
    session.execute("""CREATE INDEX idx_data ON mensagens (data_postagem);""")



    # VIEW DOS TEMAS
    session.execute("""
        CREATE MATERIALIZED VIEW IF NOT EXISTS mensagens_por_usuario_tema AS
        SELECT usuario_id, tema, data_postagem, mensagem_id
        FROM mensagens
        WHERE usuario_id IS NOT NULL 
            AND tema IS NOT NULL 
            AND data_postagem IS NOT NULL 
            AND mensagem_id IS NOT NULL
        PRIMARY KEY ((usuario_id), tema, data_postagem, mensagem_id);
    """)

    # VIEW DAS MENSAGENS_ID
    session.execute("""
        CREATE MATERIALIZED VIEW IF NOT EXISTS mensagens_por_id AS
        SELECT mensagem_id, usuario_id, data_postagem
        FROM mensagens
        WHERE mensagem_id IS NOT NULL AND usuario_id IS NOT NULL AND data_postagem IS NOT NULL
        PRIMARY KEY (mensagem_id, usuario_id, data_postagem);
    """)



# Serve para que o codigo desse arquivo seja executado apenas quando vc der o python3 database.py explicitamente ou importar e chamar sua função,
#    pois no python, se vc apenas dar o import de um arquivo ele já executa direto o codigo desse arquivo
if __name__ == "__main__":
    inicializar_database()
