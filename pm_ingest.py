from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
import uvicorn
import os

app = FastAPI(title="PM Intelligence Test")

@app.get("/")
async def root():
    return {"message": "PM Intelligence Dashboard is working!", "status": "success"}

@app.get("/test", response_class=HTMLResponse)
async def test_html():
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>PM Intelligence Test</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 40px; background: #f0f0f0; }
            .container { max-width: 800px; margin: 0 auto; background: white; padding: 40px; border-radius: 10px; }
            h1 { color: #4f46e5; }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>PM Intelligence Dashboard - Test Page</h1>
            <p>If you can see this page, the FastAPI app is working correctly!</p>
            <p><strong>Next steps:</strong> We can now add back the full functionality step by step.</p>
            <a href="/">Back to API Response</a>
        </div>
    </body>
    </html>
    """

@app.get("/health")
async def health_check():
    return {"status": "healthy", "message": "PM Intelligence API is running"}

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
