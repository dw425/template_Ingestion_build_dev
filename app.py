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
    all_analysis_results = []
    combined_data = {
        'spreadsheet_analyses': [],
        'document_analyses': [],
        'file_info': [],
        'analysis_types': []
    }
    
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
                    
                elif file_extension in ['pdf', 'docx', 'pptx']:
                    print(f"Using AI document analyzer for {filename}")
                    analyzer = AIDocumentAnalyzer()
                    analysis_result = analyzer.analyze_document(filepath)
                    analysis_result['source_file'] = filename
                    analysis_result['analysis_type'] = 'document'
                    combined_data['document_analyses'].append(analysis_result)
                
                # Add to overall results
                combined_data['file_info'].append({
                    'filename': filename,
                    'type': file_extension,
                    'size': os.path.getsize(filepath)
                })
                combined_data['analysis_types'].append(file_extension)
                all_analysis_results.append(analysis_result)
                
                # Clean up individual file
                os.remove(filepath)
                print(f"Successfully analyzed: {filename}")
                
            except Exception as e:
                print(f"Error analyzing {filename}: {str(e)}")
                if os.path.exists(filepath):
                    os.remove(filepath)
                # Continue with other files instead of failing completely
                continue
        
        if not all_analysis_results:
            return jsonify({'error': 'No files could be analyzed successfully'}), 400
        
        # Generate combined dashboard
        print("Generating combined dashboard...")
        report_gen = ReportGenerator()
        dashboard_data = report_gen.generate_combined_dashboard_data(combined_data)
        
        return jsonify({
            'success': True,
            'data': dashboard_data,
            'files_processed': len(all_analysis_results),
            'analysis_type': 'combined'
        })
        
    except Exception as e:
        print(f"Multiple file analysis error: {str(e)}")
        # Clean up any remaining files
        for file_index in range(len(files)):
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], f"{file_index}_*")
            import glob
            for f in glob.glob(filepath):
                if os.path.exists(f):
                    os.remove(f)
        
        return jsonify({'error': f'Multiple file analysis failed: {str(e)}'}), 500
