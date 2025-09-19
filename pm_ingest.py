import os
from fastapi import FastAPI
import uvicorn

app = FastAPI()

@app.get("/")
def read_root():
    return {"message": "PM Intelligence Dashboard", "status": "healthy"}

@app.get("/health")
def health_check():
    return {"status": "ok"}

@app.get("/healthz")
def health_check_k8s():
    return {"status": "ok"}

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    print(f"Starting server on port {port}")
    uvicorn.run(app, host="0.0.0.0", port=port)
