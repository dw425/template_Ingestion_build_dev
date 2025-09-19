import os
import json
import shutil
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any
from fastapi import FastAPI, File, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse
import uvicorn

# Create FastAPI app
app = FastAPI(title="PM Intelligence Dashboard")

# Storage manager
class FileStorageManager:
    def __init__(self, storage_base_path: str = "pm_storage"):
        self.base_path = Path(storage_base_path)
        self.uploads_path = self.base_path / "uploads"
        self.processed_path = self.base_path / "processed"
        self.results_path = self.base_path / "results"
        
        for path in [self.uploads_path, self.processed_path, self.results_path]:
            path.mkdir(parents=True, exist_ok=True)
    
    def save_uploaded_file(self, file: UploadFile) -> Path:
        file_path = self.uploads_path / file.filename
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        return file_path
    
    def process_all_uploaded_files(self) -> Dict[str, Any]:
        uploaded_files = list(self.uploads_path.glob("*"))
        results = {
            "total_files": len(uploaded_files),
            "successful_files": 0,
            "failed_files": 0,
            "processed_files": [],
            "errors": []
        }
        
        for file_path in uploaded_files:
            try:
                if file_path.suffix.lower() in ['.xlsx', '.xls', '.csv', '.json', '.txt', '.pptx']:
                    processed_file = self.processed_path / file_path.name
                    shutil.move(str(file_path), str(processed_file))
                    
                    results["successful_files"] += 1
                    results["processed_files"].append({
                        "filename": file_path.name,
                        "size": processed_file.stat().st_size,
                        "processed_at": datetime.now().isoformat(),
                        "status": "success"
                    })
                else:
                    results["failed_files"] += 1
                    results["errors"].append("Unsupported file type: " + file_path.name)
                    
            except Exception as e:
                results["failed_files"] += 1
                results["errors"].append("Error processing " + file_path.name + ": " + str(e))
        
        results_file = self.results_path / ("processing_results_" + datetime.now().strftime('%Y%m%d_%H%M%S') + ".json")
        with open(results_file, 'w') as f:
            json.dump(results, f, indent=2)
        
        return results
    
    def get_processed_data_summary(self) -> Dict[str, Any]:
        processed_files = list(self.processed_path.glob("*"))
        result_files = list(self.results_path.glob("*.json"))
        
        total_processed = len(processed_files)
        total_size = sum(f.stat().st_size for f in processed_files) if processed_files else 0
        
        return {
            "total_processed_files": total_processed,
            "total_size_bytes": total_size,
            "total_size_mb": round(total_size / (1024 * 1024), 2),
            "processing_sessions": len(result_files)
        }

# Initialize storage
storage_manager = FileStorageManager()

@app.get("/", response_class=HTMLResponse)
async def serve_main_interface():
    return HTMLResponse(content=get_main_html())

@app.post("/upload-files")
async def upload_files(files: List[UploadFile] = File(...)):
    try:
        for file in files:
            storage_manager.save_uploaded_file(file)
        
        results = storage_manager.process_all_uploaded_files()
        return JSONResponse(results)
        
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": "Processing failed: " + str(e)}
        )

@app.get("/dashboard", response_class=HTMLResponse)
async def analytics_dashboard():
    summary = storage_manager.get_processed_data_summary()
    return HTMLResponse(content=get_dashboard_html(summary))

@app.get("/processed-data-summary")
async def get_processed_data_summary():
    summary = storage_manager.get_processed_data_summary()
    return JSONResponse(summary)

def get_main_html():
    return '''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>PM Intelligence Dashboard</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh; color: #333;
        }
        .container { max-width: 1200px; margin: 0 auto; padding: 20px; }
        .header { text-align: center; margin-bottom: 40px; color: white; }
        .header h1 { font-size: 3rem; font-weight: 700; margin-bottom: 10px; }
        .header p { font-size: 1.2rem; opacity: 0.9; margin-bottom: 30px; }
        .main-card { background: white; border-radius: 20px; box-shadow: 0 20px 60px rgba(0,0,0,0.2); overflow: hidden; margin-bottom: 30px; }
        .card-header { background: linear-gradient(135deg, #4f46e5 0%, #7c3aed 100%); color: white; padding: 30px; text-align: center; }
        .card-header h2 { font-size: 2rem; margin-bottom: 10px; }
        .upload-area { padding: 40px; text-align: center; }
        .file-drop-zone {
            border: 3px dashed #cbd5e1; border-radius: 15px; padding: 60px 20px;
            background: #f8fafc; transition: all 0.3s ease; cursor: pointer; margin-bottom: 30px;
        }
        .file-drop-zone:hover { border-color: #4f46e5; background: #f0f4ff; }
        .upload-btn {
            background: linear-gradient(135deg, #4f46e5, #7c3aed); color: white; border: none;
            padding: 15px 30px; border-radius: 10px; font-size: 1rem; cursor: pointer;
        }
        .file-input { display: none; }
        .file-list { margin-top: 30px; text-align: left; display: none; }
        .file-item { display: flex; align-items: center; justify-content: space-between; background: #f1f5f9; border-radius: 10px; padding: 15px; margin-bottom: 10px; }
        .remove-btn { background: #ef4444; color: white; border: none; padding: 8px 15px; border-radius: 6px; cursor: pointer; }
        .process-section { padding: 30px 40px; background: #f8fafc; border-top: 1px solid #e2e8f0; }
        .process-btn {
            background: linear-gradient(135deg, #059669, #10b981); color: white; border: none;
            padding: 18px 40px; border-radius: 12px; font-size: 1.1rem; font-weight: 600;
            cursor: pointer; width: 100%; max-width: 300px; margin: 0 auto; display: block;
        }
        .process-btn:disabled { background: #9ca3af; cursor: not-allowed; }
        .progress-container { margin-top: 20px; display: none; }
        .progress-bar { width: 100%; height: 8px; background: #e2e8f0; border-radius: 4px; overflow: hidden; margin-bottom: 10px; }
        .progress-fill { height: 100%; background: linear-gradient(135deg, #059669, #10b981); width: 0%; transition: width 0.3s ease; }
        .results-section { margin-top: 30px; display: none; }
        .results-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 20px; margin-bottom: 30px; }
        .result-card { background: white; border-radius: 12px; padding: 25px; text-align: center; box-shadow: 0 4px 12px rgba(0,0,0,0.1); }
        .result-number { font-size: 2.5rem; font-weight: 700; color: #4f46e5; margin-bottom: 5px; }
        .action-btn { background: white; color: #4f46e5; border: 2px solid #4f46e5; padding: 12px 24px; border-radius: 10px; text-decoration: none; margin: 5px; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>PM Intelligence Dashboard</h1>
            <p>Upload and process your project files</p>
        </div>
        <div class="main-card">
            <div class="card-header">
                <h2>File Upload Interface</h2>
            </div>
            <div class="upload-area">
                <div class="file-drop-zone" id="dropZone">
                    <h3>Drop files here or click to browse</h3>
                    <p>Supports Excel, CSV, JSON, PowerPoint, and text files</p>
                    <button class="upload-btn" onclick="document.getElementById('fileInput').click();">Choose Files</button>
                </div>
                <input type="file" id="fileInput" class="file-input" multiple accept=".xlsx,.xls,.csv,.json,.pptx,.txt,.docx">
                <div class="file-list" id="fileList">
                    <h3>Selected Files:</h3>
                    <div id="fileItems"></div>
                </div>
            </div>
            <div class="process-section">
                <button class="process-btn" id="processBtn" disabled onclick="processFiles()">Process Files</button>
                <div class="progress-container" id="progressContainer">
                    <div class="progress-bar"><div class="progress-fill" id="progressFill"></div></div>
                    <div id="progressText">Processing...</div>
                </div>
                <div class="results-section" id="resultsSection">
                    <div class="results-grid" id="resultsGrid"></div>
                    <div>
                        <a href="/dashboard" class="action-btn">View Dashboard</a>
                        <button class="action-btn" onclick="resetInterface()">Process More Files</button>
                    </div>
                </div>
            </div>
        </div>
    </div>
    <script>
        let selectedFiles = [];
        const fileInput = document.getElementById('fileInput');
        const fileList = document.getElementById('fileList');
        const processBtn = document.getElementById('processBtn');
        
        fileInput.addEventListener('change', function(e) {
            selectedFiles = Array.from(e.target.files);
            updateFileList();
        });
        
        function updateFileList() {
            if (selectedFiles.length === 0) {
                fileList.style.display = 'none';
                processBtn.disabled = true;
                return;
            }
            fileList.style.display = 'block';
            processBtn.disabled = false;
            
            document.getElementById('fileItems').innerHTML = selectedFiles.map(function(file, index) {
                return '<div class="file-item"><span>' + file.name + '</span><button class="remove-btn" onclick="removeFile(' + index + ')">Remove</button></div>';
            }).join('');
        }
        
        function removeFile(index) {
            selectedFiles.splice(index, 1);
            updateFileList();
        }
        
        async function processFiles() {
            if (selectedFiles.length === 0) return;
            
            document.getElementById('progressContainer').style.display = 'block';
            processBtn.disabled = true;
            
            const formData = new FormData();
            selectedFiles.forEach(function(file) {
                formData.append('files', file);
            });
            
            try {
                const response = await fetch('/upload-files', {
                    method: 'POST',
                    body: formData
                });
                
                if (response.ok) {
                    const result = await response.json();
                    showResults(result);
                } else {
                    alert('Upload failed');
                }
            } catch (error) {
                alert('Error: ' + error.message);
            }
        }
        
        function showResults(results) {
            document.getElementById('resultsGrid').innerHTML = 
                '<div class="result-card"><div class="result-number">' + results.total_files + '</div><div>Files Processed</div></div>' +
                '<div class="result-card"><div class="result-number">' + results.successful_files + '</div><div>Successful</div></div>';
            document.getElementById('resultsSection').style.display = 'block';
        }
        
        function resetInterface() {
            selectedFiles = [];
            fileInput.value = '';
            updateFileList();
            document.getElementById('resultsSection').style.display = 'none';
            processBtn.disabled = true;
        }
    </script>
</body>
</html>'''

def get_dashboard_html(summary):
    total_files = str(summary.get('total_processed_files', 0))
    total_mb = str(summary.get('total_size_mb', 0))
    
    return '''<!DOCTYPE html>
<html>
<head>
    <title>PM Analytics Dashboard</title>
    <style>
        body { font-family: Arial, sans-serif; background: linear-gradient(135deg, #1e3a8a 0%, #3730a3 100%); min-height: 100vh; margin: 0; }
        .header { background: rgba(255,255,255,0.1); padding: 20px; text-align: center; color: white; }
        .container { max-width: 1200px; margin: 0 auto; padding: 20px; }
        .kpi-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 20px; margin: 20px 0; }
        .kpi-card { background: white; border-radius: 15px; padding: 30px; text-align: center; box-shadow: 0 10px 30px rgba(0,0,0,0.1); }
        .kpi-value { font-size: 3rem; font-weight: 700; color: #4f46e5; margin-bottom: 10px; }
        .nav-btn { background: #4f46e5; color: white; padding: 15px 30px; border-radius: 10px; text-decoration: none; margin: 10px; }
    </style>
</head>
<body>
    <div class="header">
        <h1>PM Intelligence Analytics</h1>
    </div>
    <div class="container">
        <div class="kpi-grid">
            <div class="kpi-card">
                <div class="kpi-value">''' + total_files + '''</div>
                <div>Files Processed</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-value">''' + total_mb + '''</div>
                <div>MB Processed</div>
            </div>
        </div>
        <div style="text-align: center;">
            <a href="/" class="nav-btn">Back to Upload</a>
            <a href="/processed-data-summary" class="nav-btn">View Data Summary</a>
        </div>
    </div>
</body>
</html>'''

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)