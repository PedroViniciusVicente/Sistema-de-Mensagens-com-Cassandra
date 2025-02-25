No docker:

(Cria a imagem do BD do Cassandra)
```
docker run --name cassandra -d -p 9042:9042 cassandra
```
(Permite interagir com o BD pelo terminal)
```
docker exec -it cassandra cqlsh
```

No VSCode:
```
pip install cassandra-driver
pip install streamlit
```

Rodar o aplicativo:
```
streamlit run app.py
```