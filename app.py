# Libs
import streamlit as st
import uuid
from datetime import datetime
import pandas as pd
import plotly.express as px


# Banco de dados
import database

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
    
    # Convertendo os dados para um DataFrame do Pandas
    mensagens_lista = []
    for msg in todasMSG:
        mensagens_lista.append({
            "Usuário ID": msg.usuario_id,
            "Data Postagem": msg.data_postagem,
            "Mensagem ID": msg.mensagem_id,
            "Idade Usuário": msg.idade_usuario,
            "Tema": msg.tema,
            "Texto": msg.texto
        })
    
    if mensagens_lista:  # Verifica se há mensagens
        df = pd.DataFrame(mensagens_lista)
        st.table(df)  # Exibe os dados de forma interativa
    else:
        st.info("Nenhuma mensagem encontrada.")





if menu == "Inserir Mensagem":
    st.subheader("Postar uma nova mensagem")

    st.write("IDs de Usuários já existentes:")
    resultado_users = session.execute("SELECT DISTINCT usuario_id FROM mensagens")
    lista_user_ids = [str(linha.usuario_id) for linha in resultado_users]

    if lista_user_ids:  # Verifica se há IDs para exibir
        df = pd.DataFrame({"Usuário ID": lista_user_ids})  # Criando um DataFrame
        st.table(df)  # Exibe como uma tabela simples
    else:
        st.info("Nenhum usuário encontrado.")

    user_id = st.text_input("ID do Usuário") # mostrar os lista_user_ids
    idade = st.number_input("Idade do Usuário", min_value=1, max_value=120)
    tema = st.selectbox("Tema", ["Política", "Saúde", "Tecnologia", "Esportes", "Música", "Cinema", "Ciência"])
    texto = st.text_area("Digite sua mensagem")

    if st.button("Enviar"):

        mensagem_id = uuid.uuid4()
        session.execute("""
            INSERT INTO mensagens (usuario_id, data_postagem, mensagem_id, idade_usuario, tema, texto)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (uuid.UUID(user_id), datetime.now(), mensagem_id, idade, tema, texto))

        # print(session)
        st.success("Mensagem enviada com sucesso! ✅")





elif menu == "Buscar Mensagens de um Usuário":
    st.subheader("Buscar mensagens de um usuário")

    st.write("IDs de Usuários já existentes:")
    resultado_users = session.execute("SELECT DISTINCT usuario_id FROM mensagens")
    lista_user_ids = [str(linha.usuario_id) for linha in resultado_users]

    if lista_user_ids:  # Verifica se há IDs para exibir
        df = pd.DataFrame({"Usuário ID": lista_user_ids})  # Criando um DataFrame
        st.table(df)  # Exibe como uma tabela simples
    else:
        st.info("Nenhum usuário encontrado.")

    user_id = st.text_input("ID do Usuário")

    if st.button("Buscar"):
        rows = session.execute("""
            SELECT * FROM mensagens WHERE usuario_id = %s
        """, (uuid.UUID(user_id),))
        
        if rows:
            for row in rows:
                st.write(f"👤 Usuário: {row.usuario_id} | **{row.idade_usuario}** anos de idade")
                st.write(f"📅 {row.data_postagem} | **{row.tema}**")
                st.write(f"📝 {row.texto}")
                st.write("---")
        else:
            st.warning("Nenhuma mensagem encontrada.")


elif menu == "Buscar Mensagem Específica":
    st.subheader("Buscar uma mensagem específica")

    st.write("IDs de Mensagens já existentes:")
    resultado_msgs = session.execute("""SELECT DISTINCT mensagem_id FROM mensagens_por_id""")
    lista_msg_ids = [str(linha.mensagem_id) for linha in resultado_msgs]

    if lista_msg_ids:  # Verifica se há IDs para exibir
        df = pd.DataFrame({"Mensagem ID": lista_msg_ids})  # Criando um DataFrame
        st.table(df)  # Exibe como uma tabela simples
    else:
        st.info("Nenhuma Mensagem encontrada.")

    mensagem_id = st.text_input("ID da Mensagem")

    if st.button("Buscar"):
        rows = session.execute("""
            SELECT * FROM mensagens WHERE mensagem_id = %s
        """, (uuid.UUID(mensagem_id),))
        
        if rows:
            for row in rows:
                st.write(f"👤 Usuário: {row.usuario_id} | **{row.idade_usuario}** anos de idade")
                st.write(f"📅 {row.data_postagem} | **{row.tema}**")
                st.write(f"📝 {row.texto}")
                st.write("---")
        else:
            st.warning("Nenhuma mensagem encontrada.")



elif menu == "Buscar Mensagens por Data":
    st.subheader("Buscar todas as mensagens em um Intervalo de Tempo Desejado")

    start_date = st.date_input("Data Inicial")
    end_date = st.date_input("Data Final")

    if st.button("Buscar"):
        rows = session.execute("""
            SELECT * FROM mensagens
            WHERE data_postagem >= %s AND data_postagem <= %s
            ALLOW FILTERING
            """, (start_date, end_date))
        
        if rows:
            for row in rows:
                st.write(f"👤 Usuário: {row.usuario_id} | **{row.idade_usuario}** anos de idade")
                st.write(f"📅 {row.data_postagem} | **{row.tema}**")
                st.write(f"📝 {row.texto}")
                st.write("---")
        else:
            st.warning("Nenhuma mensagem encontrada.")



elif menu == "Frequência de Temas":
    # TALVEZ PLOTAR UM GRAFICO COM AS FREQUENCIAS :D
    result = session.execute("""
        SELECT usuario_id, tema, COUNT(*) AS frequencia
        FROM mensagens_por_usuario_tema
        GROUP BY usuario_id, tema;
    """)

    # for row in result:
    #     st.write(f"Usuário: {row.usuario_id}, Tema: {row.tema}, Frequência: {row.frequencia}")

    # Convertendo para uma lista de dicionários, transformando UUID em string
    dados = [{"Usuário ID": str(row.usuario_id), "Tema": row.tema, "Frequência": row.frequencia} for row in result]

    # Verifica se há dados
    if dados:
        df = pd.DataFrame(dados)
        
        # 🔹 Exibir tabela estruturada
        st.table(df)

        # 🔹 Plotar gráfico de barras - Frequência dos temas
        st.subheader("Distribuição de Temas")
        fig_tema = px.bar(df, x="Tema", y="Frequência", color="Tema", title="Frequência dos Temas", text_auto=True)
        st.plotly_chart(fig_tema, use_container_width=True)

        # 🔹 Plotar gráfico de barras - Frequência por Usuário e Tema
        st.subheader("Frequência de Mensagens por Usuário e Tema")
        fig_usuario_tema = px.bar(df, x="Usuário ID", y="Frequência", color="Tema",
                                title="Frequência de Temas por Usuário",
                                barmode="group", text_auto=True)
        st.plotly_chart(fig_usuario_tema, use_container_width=True)
    else:
        st.info("Nenhuma mensagem encontrada.")
