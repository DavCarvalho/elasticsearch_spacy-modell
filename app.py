from fastapi import FastAPI, HTTPException
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError
from PyPDF2 import PdfReader
from indexMapping import indexMapping
import os 

app = FastAPI()

def connect_to_elasticsearch(elasticsearch_url):
    try:
        # Conectar ao Elasticsearch
        es = Elasticsearch([elasticsearch_url])

        # Verificar a conexão obtendo informações do cluster
        info = es.info()
        print("Conexão bem-sucedida com o Elasticsearch:")
        print("Nome do cluster:", info['cluster_name'])
        print("Versão do Elasticsearch:", info['version']['number'])
        return es
    except ConnectionError:
        raise ConnectionError("Erro: Não foi possível conectar ao Elasticsearch em " + elasticsearch_url)
    except Exception as e:
        raise Exception("Erro desconhecido: " + str(e))

elasticsearch_url = "http://localhost:9200"
es = connect_to_elasticsearch(elasticsearch_url)

index_name = "kempetro"

index_exists = es.indices.exists(index=index_name)

if not index_exists:
    es.indices.create(index=index_name, body=indexMapping)


def extrair_texto_pdf_index_elasticsearch(folder_path, index_name):
  for filename in os.listdir(folder_path):
    if filename.endswith(".PDF"):
        file_path = os.path.join(folder_path, filename)

          # Extrair o texto do PDF
        try:
            reader = PdfReader(file_path)
            texts = [page.extract_text() for page in reader.pages]
        except Exception as e:
            print(f"Erro ao ler o PDF {filename}: {e}")
            continue

        # Indexar o texto no Elasticsearch
        for i, text in enumerate(texts):
            try:
                document = {
                    "fileName": filename,
                    "page": i,
                    "content": text
                }

                es.index(index=index_name, body=document)
                print("Documento indexado:", document)
            except Exception as e:
                 print(f"Erro ao indexar o documento {filename}: {e}")

  print("Indexação concluída com sucesso!")



@app.get("/")
def home():
    return {"message": "Hello World"}

@app.post("/create_index")
def crate_index(index_name: str):
    try:
        index_exists = es.indices.exists(index=index_name)
        if not index_exists:
            es.indices.create(index=index_name, body=indexMapping)
        return {"message": "Índice criado com sucesso!"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Erro ao criar o índice {index_name}: {e}")


@app.get("/all_indexes")
def all_indexes():
    try:
        indexes = es.indices.get_alias()
        return indexes
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Erro ao listar os índices: {e}")



@app.delete("/delete_index")
def delete_index(index_name: str):
    try:
        es.indices.delete(index=index_name)
        return {"message": "Índice deletado com sucesso!"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Erro ao deletar o índice {index_name}: {e}")


@app.post("/indexar_documento")
def indexar_documento(index_name: str):
    folder_path = "./pdf"
    extrair_texto_pdf_index_elasticsearch(folder_path, index_name)
   
    return {"message": "Indexação concluída com sucesso!"}


@app.get("/search/{query}")
def buscar_palavras(query: str):
    query_body = {
        "query": {
            "match": {
                "content": {
                    "query": query,
                    "operator": "and",
                    "fuzziness": "auto"
                }
            }
        }
    }

    try:
        # Executar a pesquisa
        result = es.search(index=index_name, body=query_body)
        
        # Extrair informações relevantes do resultado
        hits = result["hits"]["hits"]
        total_hits = result["hits"]["total"]["value"]
        
        return {"total_hits": total_hits, "hits": hits}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao realizar a pesquisa no índice {index_name}: {e}")


# Execução
if __name__ == "__main__":
    import uvicorn
    
    # Adicionar o host = '0.0.0.0' após produção. Remover o reload.
    uvicorn.run("app:app", port=8000, reload=True)  

