import os
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import uvicorn
import traceback

app = FastAPI()

@app.middleware("http")
async def catch_exceptions_middleware(request: Request, call_next):
    try:
        response = await call_next(request)
        return response
    except Exception as e:
        print(f"Exception caught: {e}")
        print(f"Traceback: {traceback.format_exc()}")
        return JSONResponse(
            status_code=500,
            content={"error": "Internal server error", "details": str(e)}
        )

@app.get("/")
async def read_root():
    try:
        return {"message": "PM Intelligence Dashboard", "status": "healthy", "timestamp": "working"}
    except Exception as e:
        print(f"Error in root endpoint: {e}")
        raise

@app.get("/health")
async def health_check():
    return {"status": "ok", "service": "pm-intelligence"}

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    print(f"Starting server on port {port}")
    uvicorn.run(app, host="0.0.0.0", port=port)
