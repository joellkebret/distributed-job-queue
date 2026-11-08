from fastapi import FastAPI

app = FastAPI(
    title="Distributed Job Queue",
    description="Asynchronous job processing using Redis Streams"
)


@app.get("/health")
def health():
    return {"Status: OK"}

