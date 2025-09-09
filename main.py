from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from opensearchpy import OpenSearch, NotFoundError

INDEX = "books"

app = FastAPI(title="OpenSearch + FastAPI demo")

def os_client() -> OpenSearch:
    # security отключён — подключаемся по http без auth
    return OpenSearch(
        hosts=[{"host": "localhost", "port": 9200}],
        use_ssl=False,
        verify_certs=False,
        ssl_assert_hostname=False,
        ssl_show_warn=False,
        http_compress=True,
    )

def ensure_index():
    client = os_client()
    if not client.indices.exists(INDEX):
        body = {
            "settings": {
                "index": {"number_of_shards": 1, "number_of_replicas": 0}
            },
            "mappings": {
                "properties": {
                    "title":   {"type": "text"},
                    "author":  {"type": "text"},
                    "year":    {"type": "integer"},
                    "tags":    {"type": "keyword"}
                }
            },
        }
        client.indices.create(index=INDEX, body=body)

class Book(BaseModel):
    title: str
    author: str
    year: int | None = None
    tags: list[str] = []

@app.on_event("startup")
def _startup():
    ensure_index()

@app.get("/")
def health():
    client = os_client()
    info = client.info()
    return {"ok": True, "cluster": info.get("cluster_name")}

@app.post("/books/{id}")
def upsert_book(id: str, book: Book):
    client = os_client()
    res = client.index(index=INDEX, id=id, body=book.model_dump(), refresh=True)
    return {"result": res["result"], "_id": res["_id"]}

@app.get("/books/{id}")
def get_book(id: str):
    client = os_client()
    try:
        res = client.get(index=INDEX, id=id)
        return res["_source"] | {"_id": res["_id"]}
    except NotFoundError:
        raise HTTPException(status_code=404, detail="not found")

@app.get("/search")
def search(q: str, size: int = 5):
    client = os_client()
    body = {
        "size": size,
        "query": {
            "multi_match": {
                "query": q,
                "fields": ["title^2", "author"]
            }
        }
    }
    res = client.search(index=INDEX, body=body)
    return [
        hit["_source"] | {"_id": hit["_id"], "_score": hit["_score"]}
        for hit in res["hits"]["hits"]
    ]
