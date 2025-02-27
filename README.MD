No docker:

(Cria a imagem do BD do Cassandra)
```
docker run --name cassandra -d -p 9042:9042 cassandra
```
(Permite interagir com o BD pelo terminal, não é necessário para esta aplicação)
```
docker exec -it cassandra cqlsh
```

No VSCode:
```
pip install cassandra-driver
pip install streamlit
pip install pandas
pip install plotly-express
```

Rodar o aplicativo:
```
streamlit run app.py
```