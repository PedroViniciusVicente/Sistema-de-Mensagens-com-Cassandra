# Libs
import streamlit as st
import uuid
from datetime import datetime

# Banco de dados
import database

# Paginas



# Verifica se já inicializou o banco de dados (inicializar apenas 1 vez, e não em cada operacao)
if "db_initialized" not in st.session_state:
    database.inicializar_database()  # Faz a conexao, cria e usa keyspace, cria e povoa a table e cria os indexes
    st.session_state["db_initialized"] = True  # Marca como inicializado

session = database.session


############### TITULOS E SIDEBAR ###############
st.title("📩 Gerenciador de Mensagens - Cassandra")
st.write("Feito por Guilherme e Pedro")

menu = st.sidebar.radio("Selecione uma opção:", [
    "Listar Todas Mensagens",
    "Inserir Mensagem",
    "Buscar Mensagens de um Usuário",
    "Buscar Mensagem Específica",
    "Buscar Mensagens por Data",
    "Frequência de Temas"])

############### INSERIR MENSAGEM ###############

if menu == "Listar Todas Mensagens":
    st.subheader("Listando todas as mensagens existentes")
    todasMSG = session.execute(""" SELECT * FROM mensagens; """)
    for mgsIndividual in todasMSG:
        st.write(mgsIndividual)

    # statement = SimpleStatement("SELECT * FROM users", fetch_size=10)
    # for user_row in session.execute(statement):
    #      process_user(user_row)


if menu == "Inserir Mensagem":
    st.subheader("Postar uma nova mensagem")

    st.write("IDs de Usuários já existentes:")
    resultado = session.execute("SELECT DISTINCT usuario_id FROM mensagens")
    lista_user_ids = [str(linha.usuario_id) for linha in resultado]
    
    for usuarioIndividual in lista_user_ids:
        st.write(usuarioIndividual)

    # user_id = st.text_input("ID do Usuário", str(uuid.uuid4()))
    user_id = st.text_input("ID do Usuário", "aa") # AO INVES DE AA, MOSTRAR TODOS OS IDS DE TODOS OS USUARIOS EXISTENTES
    idade = st.number_input("Idade do Usuário", min_value=1, max_value=120)
    tema = st.selectbox("Tema", ["política", "saúde", "tecnologia"])
    texto = st.text_area("Digite sua mensagem")

    if st.button("Enviar"):

        mensagem_id = uuid.uuid4()
        session.execute("""
            INSERT INTO mensagens (usuario_id, data_postagem, mensagem_id, idade_usuario, tema, texto)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (uuid.UUID(user_id), datetime.now(), mensagem_id, idade, tema, texto))

        # session.execute("""
        #     UPDATE mensagens_por_tema
        #     SET frequencia = frequencia + 1
        #     WHERE usuario_id = %s AND tema = %s
        # """, (uuid.UUID(user_id), tema))

        # print(session)
        st.success("Mensagem enviada com sucesso! ✅")





elif menu == "Buscar Mensagens de um Usuário":
    st.subheader("Buscar mensagens de um usuário")

    user_id = st.text_input("ID do Usuário")

    if st.button("Buscar"):
        rows = session.execute("""
            SELECT * FROM mensagens WHERE usuario_id = %s
        """, (uuid.UUID(user_id),))
        
        if rows:
            for row in rows:
                st.write(f"📅 {row.data_postagem} | **{row.tema}**")
                st.write(f"📝 {row.texto}")
                st.write("---")
        else:
            st.warning("Nenhuma mensagem encontrada.")


elif menu == "Buscar Mensagem Específica":
    st.subheader("Buscar uma mensagem específica")

elif menu == "Buscar Mensagens por Data":
    st.subheader("Buscar todas as mensagens em um Intervalo de Tempo Desejado")


elif menu == "Frequência de Temas":
    # TALVEZ PLOTAR UM GRAFICO COM AS FREQUENCIAS :D
    result = session.execute("""
        SELECT usuario_id, tema, COUNT(*) AS frequencia
        FROM mensagens_por_usuario_tema
        GROUP BY usuario_id, tema;
    """)

    for row in result:
        st.write(f"Usuário: {row.usuario_id}, Tema: {row.tema}, Frequência: {row.frequencia}")
