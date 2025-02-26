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

    insert_mensagem(usuario1, uuid.uuid4(), datetime.now(), 25, "Esportes", "O menino Ney tá jogando muito!")
    insert_mensagem(usuario3, uuid.uuid4(), datetime.now() - timedelta(days=2), 15, "Música", "Adoro as músicas do Mozart.")
    insert_mensagem(usuario2, uuid.uuid4(), datetime.now() - timedelta(days=1), 61, "Tecnologia", "O novo framework do JavaScript é EXCELENTE!")
    insert_mensagem(usuario1, uuid.uuid4(), datetime.now() - timedelta(days=3), 25, "Cinema", "Gostei do filme novo do Sonic.")
    insert_mensagem(usuario2, uuid.uuid4(), datetime.now() - timedelta(days=4), 61, "Ciência", "Qual a importancia do estudo das nano particulas?")
    insert_mensagem(usuario4, uuid.uuid4(), datetime.now() - timedelta(days=5), 42, "Esportes", "O Michael Jordan fez uma boa carreira no basquete.")
    insert_mensagem(usuario4, uuid.uuid4(), datetime.now() - timedelta(days=6), 42, "Tecnologia", "Mensagem de exemplo número 7 sobre Tecnologia.")
    insert_mensagem(usuario3, uuid.uuid4(), datetime.now() - timedelta(days=7), 15, "Música", "Aprecio o som do Bob Dylan.")
    insert_mensagem(usuario4, uuid.uuid4(), datetime.now() - timedelta(days=8), 42, "Saúde", "Milhares de médicos se formaram no Brasil.")
    insert_mensagem(usuario1, uuid.uuid4(), datetime.now() - timedelta(days=9), 29, "Ciência", "Os inventos de Marie Curie são importantes ainda hoje.")
    insert_mensagem(usuario2, uuid.uuid4(), datetime.now() - timedelta(days=10), 61, "Política", "As eleições ocorreram normalmente na Alemanha.")
    insert_mensagem(usuario3, uuid.uuid4(), datetime.now() - timedelta(days=11), 15, "Tecnologia", "A nova GPU da NVidia é extremamente inovadora!")
    insert_mensagem(usuario5, uuid.uuid4(), datetime.now() - timedelta(days=12), 19, "Música", "Nesse carnaval teremos novos ritmos e sons.")
    insert_mensagem(usuario1, uuid.uuid4(), datetime.now() - timedelta(days=13), 25, "Cinema", "Vou assistir ao Charlie Chaplin amanhã.")
    insert_mensagem(usuario2, uuid.uuid4(), datetime.now() - timedelta(days=14), 61, "Ciência", "Um jovem doutor recebeu o Nobel de Medicina.")
    insert_mensagem(usuario5, uuid.uuid4(), datetime.now() - timedelta(days=15), 19, "Esportes", "O campeonato de Xadrez terminou com a vitŕoa de Gukesh.")
    insert_mensagem(usuario5, uuid.uuid4(), datetime.now() - timedelta(days=16), 19, "Tecnologia", "Em breve teremos carros voadores!")
    insert_mensagem(usuario3, uuid.uuid4(), datetime.now() - timedelta(days=17), 15, "Música", "A OJESP vai se apresentar amanhã no Teatro São Paulo.")
    insert_mensagem(usuario1, uuid.uuid4(), datetime.now() - timedelta(days=18), 25, "Cinema", "Adorei o filme do Pelé!")
    insert_mensagem(usuario2, uuid.uuid4(), datetime.now() - timedelta(days=19), 61, "Ciência", "Novo vírus foi identificado na índia na tarde de ontem.")
    insert_mensagem(usuario2, uuid.uuid4(), datetime.now() - timedelta(days=20), 61, "Política", "Os democratas obteram maioria no parlamento.")





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
