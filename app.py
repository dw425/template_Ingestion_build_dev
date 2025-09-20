from flask import Flask, render_template, request, jsonify, redirect, url_for
import os
import pandas as pd
import json
import glob
from werkzeug.utils import secure_filename
from analyzers.spreadsheet_analyzer import SpreadsheetAnalyzer
from analyzers.report_generator import ReportGenerator

try:
    from analyzers.ai_document_analyzer import AIDocumentAnalyzer
    AI_AVAILABLE = True
except ImportError:
    AI_AVAILABLE = False
    print("AI document analyzer not available - will use basic analysis")

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['MAX_CONTENT_LENGTH'] = 32 * 1024 * 1024  # 32MB for documents

os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

ALLOWED_EXTENSIONS = {'xlsx', 'xls', 'csv', 'json', 'pdf', 'docx', 'pptx'}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/')
def dashboard():
    return render_template('dashboard.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    print("=== SINGLE FILE UPLOAD ROUTE CALLED ===")
    print(f"Request files: {request.files}")
    print(f"Request method: {request.method}")
    
    if 'file' not in request.files:
        print("No file in request")
        return jsonify({'error': 'No file selected'}), 400
    
    file = request.files['file']
    print(f"File received: {file.filename}")
    
    if file.filename == '':
        print("Empty filename")
        return jsonify({'error': 'No file selected'}), 400
    
    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        print(f"Saving file to: {filepath}")
        file.save(filepath)
        
        try:
            print("Starting analysis...")
            
            # Determine file type and use appropriate analyzer
            file_extension = filename.lower().split('.')[-1]
            print(f"DEBUG: File extension detected: '{file_extension}'")
            print(f"DEBUG: Checking if '{file_extension}' in ['xlsx', 'xls', 'csv', 'json']")
            
            if file_extension in ['xlsx', 'xls', 'csv', 'json']:
                # Use spreadsheet analyzer for data files
                print(f"DEBUG: Using spreadsheet analyzer for {file_extension}")
                analyzer = SpreadsheetAnalyzer()
                analysis_results = analyzer.analyze_file(filepath)
                analysis_type = "spreadsheet"
                
            elif file_extension in ['pdf', 'docx', 'pptx'] and AI_AVAILABLE:
                # Use AI document analyzer for documents
                print("Using AI document analyzer...")
                analyzer = AIDocumentAnalyzer()
                analysis_results = analyzer.analyze_document(filepath)
                analysis_type = "document"
                
            elif file_extension in ['pdf', 'docx', 'pptx'] and not AI_AVAILABLE:
                raise Exception("Document analysis requires AI libraries. Please set up API keys or use spreadsheet files.")
                
            else:
                raise Exception(f"Unsupported file type: {file_extension}")
            
            print("Analysis complete!")
            
            # Generate dashboard data
            report_gen = ReportGenerator()
            dashboard_data = report_gen.generate_dashboard_data(analysis_results, analysis_type)
            print("Report generation complete!")
            
            # Clean up
            os.remove(filepath)
            
            return jsonify({
                'success': True,
                'data': dashboard_data,
                'analysis_type': analysis_type
            })
            
        except Exception as e:
            print(f"Analysis error: {str(e)}")
            if os.path.exists(filepath):
                os.remove(filepath)
            return jsonify({'error': f'Analysis failed: {str(e)}'}), 500
    
    print("Invalid file type")
    return jsonify({'error': 'Invalid file type. Supported: Excel, CSV, JSON, PDF, Word, PowerPoint'}), 400

@app.route('/upload-multiple', methods=['POST'])
def upload_multiple_files():
    print("=== MULTIPLE FILE UPLOAD ROUTE CALLED ===")
    
    if 'files' not in request.files:
        return jsonify({'error': 'No files selected'}), 400
    
    files = request.files.getlist('files')
    print(f"Number of files received: {len(files)}")
    
    if not files or all(file.filename == '' for file in files):
        return jsonify({'error': 'No files selected'}), 400
    
    # Store all analysis results
    combined_data = {
        'spreadsheet_analyses': [],
        'document_analyses': [],
        'file_info': [],
        'analysis_types': []
    }
    
    processed_files = 0
    
    try:
        for file_index, file in enumerate(files):
            if not file or file.filename == '':
                continue
                
            if not allowed_file(file.filename):
                print(f"Skipping unsupported file: {file.filename}")
                continue
            
            filename = secure_filename(file.filename)
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], f"{file_index}_{filename}")
            print(f"Processing file {file_index + 1}: {filename}")
            
            file.save(filepath)
            
            try:
                # Determine file type and analyze
                file_extension = filename.lower().split('.')[-1]
                
                if file_extension in ['xlsx', 'xls', 'csv', 'json']:
                    print(f"Using spreadsheet analyzer for {filename}")
                    analyzer = SpreadsheetAnalyzer()
                    analysis_result = analyzer.analyze_file(filepath)
                    analysis_result['source_file'] = filename
                    analysis_result['analysis_type'] = 'spreadsheet'
                    combined_data['spreadsheet_analyses'].append(analysis_result)
                    
                elif file_extension in ['pdf', 'docx', 'pptx'] and AI_AVAILABLE:
                    print(f"Using AI document analyzer for {filename}")
                    analyzer = AIDocumentAnalyzer()
                    analysis_result = analyzer.analyze_document(filepath)
                    analysis_result['source_file'] = filename
                    analysis_result['analysis_type'] = 'document'
                    combined_data['document_analyses'].append(analysis_result)
                    
                elif file_extension in ['pdf', 'docx', 'pptx'] and not AI_AVAILABLE:
                    print(f"Skipping {filename} - AI analysis not available")
                    continue
                
                # Add to file info
                combined_data['file_info'].append({
                    'filename': filename,
                    'type': file_extension,
                    'size': os.path.getsize(filepath)
                })
                combined_data['analysis_types'].append(file_extension)
                processed_files += 1
                
                # Clean up individual file
                os.remove(filepath)
                print(f"Successfully analyzed: {filename}")
                
            except Exception as e:
                print(f"Error analyzing {filename}: {str(e)}")
                if os.path.exists(filepath):
                    os.remove(filepath)
                # Continue with other files instead of failing completely
                continue
        
        if processed_files == 0:
            return jsonify({'error': 'No files could be analyzed successfully'}), 400
        
        # Generate combined dashboard
        print("Generating combined dashboard...")
        report_gen = ReportGenerator()
        
        # Check if we have the combined dashboard method
        if hasattr(report_gen, 'generate_combined_dashboard_data'):
            dashboard_data = report_gen.generate_combined_dashboard_data(combined_data)
        else:
            # Fallback to regular dashboard with first analysis
            if combined_data['spreadsheet_analyses']:
                dashboard_data = report_gen.generate_dashboard_data(
                    combined_data['spreadsheet_analyses'][0], 'spreadsheet'
                )
            elif combined_data['document_analyses']:
                dashboard_data = report_gen.generate_dashboard_data(
                    combined_data['document_analyses'][0], 'document'
                )
            else:
                return jsonify({'error': 'No valid analyses generated'}), 400
        
        return jsonify({
            'success': True,
            'data': dashboard_data,
            'files_processed': processed_files,
            'analysis_type': 'combined'
        })
        
    except Exception as e:
        print(f"Multiple file analysis error: {str(e)}")
        
        # Clean up any remaining files
        cleanup_pattern = os.path.join(app.config['UPLOAD_FOLDER'], "*_*")
        for f in glob.glob(cleanup_pattern):
            try:
                os.remove(f)
            except:
                pass
        
        return jsonify({'error': f'Multiple file analysis failed: {str(e)}'}), 500

@app.route('/health')
def health_check():
    return {'status': 'healthy', 'ai_available': AI_AVAILABLE}

@app.route('/test')
def test():
    return "Flask app is working!"

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)
