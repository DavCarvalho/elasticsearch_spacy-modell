from fastapi import FastAPI, HTTPException
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError
from PyPDF2 import PdfReader
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




def extrair_texto_pdf(folder_path):
    resultados = []  # Lista para armazenar os resultados

    # Para cada arquivo em folder_path
    for filename in os.listdir(folder_path):
        # Verificar se termina com .pdf
        if filename.lower().endswith(".pdf"):
            # Pegar o path do arquivo
            pdf_path = os.path.join(folder_path, filename)

            # Criar um contexto para garantir que o arquivo será fechado
            with open(pdf_path, 'rb') as f:
                # Criar um leitor de PDF
                pdf = PdfReader(f)

                # Iterar sobre as páginas e extrair o texto
                for page_num, page in enumerate(pdf.pages):
                    text = page.extract_text()
                    # Adicionar informações a resultados
                    resultados.append({
                        "arquivo": filename,
                        "pagina": page_num,
                        "texto": text
                    })

    return resultados
                






@app.get("/search")
async def search():
    folder_path = "./pdf"
    return {"testes textos": extrair_texto_pdf(folder_path)}




# Execução
if __name__ == "__main__":
    import uvicorn
    
    # Adicionar o host = '0.0.0.0' após produção. Remover o reload.
    uvicorn.run("app:app", port=8000, reload=True)  

