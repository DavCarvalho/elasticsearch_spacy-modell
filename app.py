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



@app.get("/search")
def buscar_textos():
        






# Execução
if __name__ == "__main__":
    import uvicorn
    
    # Adicionar o host = '0.0.0.0' após produção. Remover o reload.
    uvicorn.run("app:app", port=8000, reload=True)  

