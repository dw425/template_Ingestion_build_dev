// Enhanced JavaScript for multiple file uploads

console.log("JavaScript loaded successfully!");

document.addEventListener('DOMContentLoaded', function() {
    console.log("DOM loaded, setting up events");
    
    const fileInput = document.getElementById('file-input');
    const browseBtn = document.getElementById('browse-btn');
    const uploadBtn = document.getElementById('upload-btn');
    const fileInfo = document.getElementById('file-info');
    const fileList = document.getElementById('file-list');
    const fileCount = document.getElementById('file-count');
    
    let selectedFiles = [];
    
    console.log("Elements found:", {
        fileInput: !!fileInput,
        browseBtn: !!browseBtn,
        uploadBtn: !!uploadBtn,
        fileInfo: !!fileInfo
    });
    
    // Browse button click
    if (browseBtn) {
        browseBtn.addEventListener('click', function() {
            console.log("Browse button clicked");
            if (fileInput) {
                fileInput.click();
            }
        });
    }
    
    // File input change - handle multiple files
    if (fileInput) {
        fileInput.addEventListener('change', function(e) {
            console.log("Files selected:", e.target.files);
            selectedFiles = Array.from(e.target.files);
            
            if (selectedFiles.length > 0) {
                displaySelectedFiles();
                showFileInfo();
            }
        });
    }
    
    // Drag and drop support for multiple files
    const dropZone = document.getElementById('drop-zone');
    if (dropZone) {
        dropZone.addEventListener('dragover', function(e) {
            e.preventDefault();
            dropZone.classList.add('dragover');
        });
        
        dropZone.addEventListener('dragleave', function() {
            dropZone.classList.remove('dragover');
        });
        
        dropZone.addEventListener('drop', function(e) {
            e.preventDefault();
            dropZone.classList.remove('dragover');
            
            selectedFiles = Array.from(e.dataTransfer.files);
            if (selectedFiles.length > 0) {
                displaySelectedFiles();
                showFileInfo();
            }
        });
    }
    
    // Upload button click
    if (uploadBtn) {
        uploadBtn.addEventListener('click', function() {
            console.log("Upload button clicked");
            
            if (selectedFiles.length === 0) {
                console.log("No files selected");
                alert("Please select files first");
                return;
            }
            
            console.log("Starting upload for multiple files:", selectedFiles.map(f => f.name));
            uploadMultipleFiles();
        });
    }
    
    function displaySelectedFiles() {
        if (!fileList) return;
        
        fileList.innerHTML = '';
        
        selectedFiles.forEach((file, index) => {
            const fileItem = document.createElement('div');
            fileItem.className = 'flex items-center justify-between p-2 bg-white rounded border';
            
            const fileIcon = getFileIcon(file.name);
            const fileSize = formatFileSize(file.size);
            
            fileItem.innerHTML = `
                <div class="flex items-center">
                    <i class="fas fa-${fileIcon} text-blue-600 mr-2"></i>
                    <span class="text-sm font-medium">${file.name}</span>
                    <span class="text-xs text-gray-500 ml-2">(${fileSize})</span>
                </div>
                <button class="text-red-500 hover:text-red-700" onclick="removeFile(${index})">
                    <i class="fas fa-times"></i>
                </button>
            `;
            
            fileList.appendChild(fileItem);
        });
        
        updateFileCount();
    }
    
    function showFileInfo() {
        if (fileInfo) {
            fileInfo.classList.remove('hidden');
        }
    }
    
    function updateFileCount() {
        if (fileCount) {
            const totalSize = selectedFiles.reduce((sum, file) => sum + file.size, 0);
            fileCount.textContent = `${selectedFiles.length} files selected (${formatFileSize(totalSize)} total)`;
        }
    }
    
    function getFileIcon(filename) {
        const ext = filename.toLowerCase().split('.').pop();
        const iconMap = {
            'xlsx': 'file-excel',
            'xls': 'file-excel',
            'csv': 'file-csv',
            'json': 'file-code',
            'pdf': 'file-pdf',
            'docx': 'file-word',
            'pptx': 'file-powerpoint'
        };
        return iconMap[ext] || 'file';
    }
    
    function formatFileSize(bytes) {
        if (bytes === 0) return '0 Bytes';
        const k = 1024;
        const sizes = ['Bytes', 'KB', 'MB', 'GB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
    }
    
    // Global function to remove files
    window.removeFile = function(index) {
        selectedFiles.splice(index, 1);
        displaySelectedFiles();
        
        if (selectedFiles.length === 0) {
            fileInfo.classList.add('hidden');
        }
    };
    
    function uploadMultipleFiles() {
        // Show loading
        document.getElementById('upload-section').classList.add('hidden');
        document.getElementById('loading-section').classList.remove('hidden');
        
        // Create form data with multiple files
        const formData = new FormData();
        selectedFiles.forEach((file, index) => {
            formData.append(`files`, file);
        });
        
        // Upload files
        fetch('/upload-multiple', {
            method: 'POST',
            body: formData
        })
        .then(response => {
            console.log("Response status:", response.status);
            return response.json();
        })
        .then(data => {
            console.log("Response data:", data);
            
            if (data.success) {
                alert("Multiple file upload successful! Check console for data.");
                console.log("Combined analysis results:", data.data);
                
                // Display the combined dashboard
                displayCombinedAnalysis(data.data);
                
                // Show dashboard content
                document.getElementById('dashboard-content').classList.remove('hidden');
            } else {
                alert("Upload failed: " + (data.error || "Unknown error"));
            }
            
            // Hide loading
            document.getElementById('upload-section').classList.remove('hidden');
            document.getElementById('loading-section').classList.add('hidden');
        })
        .catch(error => {
            console.error("Upload error:", error);
            alert("Upload failed: " + error.message);
            
            // Hide loading
            document.getElementById('upload-section').classList.remove('hidden');
            document.getElementById('loading-section').classList.add('hidden');
        });
    }
    
    function displayCombinedAnalysis(dashboardData) {
        console.log("Displaying combined analysis results");
        
        // Show appropriate sections based on analysis type
        if (dashboardData.analysis_type === 'combined') {
            // Show both AI analysis and regular dashboard sections
            document.getElementById('ai-analysis-section').classList.remove('hidden');
        }
        
        // Render dashboard components
        renderDashboardComponents(dashboardData);
    }
});

// Display functions for different analysis types (keeping existing functions)
function displayAIAnalysis(dashboardData) {
    console.log("Displaying AI analysis results");
    
    // Show AI analysis section
    document.getElementById('ai-analysis-section').classList.remove('hidden');
    
    // Populate AI analysis sections
    const aiAnalysis = dashboardData.ai_analysis || {};
    
    // Project Overview
    const projectOverview = aiAnalysis.project_overview || {};
    document.getElementById('project-overview-content').innerHTML = `
        <p><strong>Name:</strong> ${projectOverview.project_name || 'Unknown'}</p>
        <p><strong>Type:</strong> ${projectOverview.project_type || 'Unknown'}</p>
        <p><strong>Description:</strong> ${projectOverview.description || 'No description available'}</p>
        ${projectOverview.objectives && projectOverview.objectives.length > 0 ? 
            `<p><strong>Objectives:</strong> ${projectOverview.objectives.slice(0, 3).join(', ')}</p>` : ''}
    `;
    
    // Timeline
    const timelineInfo = aiAnalysis.timeline_info || {};
    document.getElementById('timeline-content').innerHTML = `
        ${timelineInfo.start_date ? `<p><strong>Start:</strong> ${timelineInfo.start_date}</p>` : ''}
        ${timelineInfo.end_date ? `<p><strong>End:</strong> ${timelineInfo.end_date}</p>` : ''}
        ${timelineInfo.milestones && timelineInfo.milestones.length > 0 ? 
            `<p><strong>Milestones:</strong> ${timelineInfo.milestones.slice(0, 3).join(', ')}</p>` : ''}
        ${timelineInfo.deadlines && timelineInfo.deadlines.length > 0 ? 
            `<p><strong>Deadlines:</strong> ${timelineInfo.deadlines.slice(0, 2).join(', ')}</p>` : ''}
    `;
    
    // KPIs & Metrics
    const kpis = aiAnalysis.kpis_and_metrics || {};
    document.getElementById('kpis-content').innerHTML = `
        ${kpis.performance_metrics && kpis.performance_metrics.length > 0 ? 
            `<p><strong>Metrics:</strong> ${kpis.performance_metrics.slice(0, 3).join(', ')}</p>` : ''}
        ${kpis.numerical_targets && kpis.numerical_targets.length > 0 ? 
            `<p><strong>Targets:</strong> ${kpis.numerical_targets.slice(0, 3).join(', ')}</p>` : ''}
        ${kpis.success_criteria && kpis.success_criteria.length > 0 ? 
            `<p><strong>Success Criteria:</strong> ${kpis.success_criteria.slice(0, 2).join(', ')}</p>` : ''}
    `;
    
    // Team & Resources
    const teamInfo = aiAnalysis.team_and_resources || {};
    document.getElementById('team-content').innerHTML = `
        ${teamInfo.team_members && teamInfo.team_members.length > 0 ? 
            `<p><strong>Team:</strong> ${teamInfo.team_members.slice(0, 5).join(', ')}</p>` : ''}
        ${teamInfo.roles && teamInfo.roles.length > 0 ? 
            `<p><strong>Roles:</strong> ${teamInfo.roles.slice(0, 3).join(', ')}</p>` : ''}
        ${teamInfo.budget_info && teamInfo.budget_info !== 'Not available' ? 
            `<p><strong>Budget:</strong> ${teamInfo.budget_info}</p>` : ''}
    `;
    
    // Also render regular dashboard components
    renderDashboardComponents(dashboardData);
}

function displaySpreadsheetAnalysis(dashboardData) {
    console.log("Displaying spreadsheet analysis results");
    
    // Hide AI analysis section for spreadsheet data
    document.getElementById('ai-analysis-section').classList.add('hidden');
    
    // Render regular dashboard components
    renderDashboardComponents(dashboardData);
}

function renderDashboardComponents(dashboardData) {
    // Render summary cards
    renderSummaryCards(dashboardData.summary_cards || []);
    
    // Render charts
    renderCharts(dashboardData.charts || []);
    
    // Render insights
    renderInsights(dashboardData.insights || []);
    
    // Render raw data
    renderRawData(dashboardData.raw_data || {});
}

function renderSummaryCards(cards) {
    const container = document.getElementById('summary-cards');
    container.innerHTML = '';
    
    cards.forEach(card => {
        const cardElement = document.createElement('div');
        cardElement.className = 'summary-card bg-white rounded-lg shadow p-6';
        cardElement.innerHTML = `
            <div class="flex items-center">
                <div class="flex-shrink-0">
                    <i class="fas fa-${getIconClass(card.icon)} text-2xl text-blue-600"></i>
                </div>
                <div class="ml-4">
                    <p class="text-sm font-medium text-gray-500">${card.title}</p>
                    <p class="text-2xl font-bold text-gray-900">${card.value}</p>
                    <p class="text-sm text-gray-600">${card.subtitle}</p>
                </div>
            </div>
        `;
        container.appendChild(cardElement);
    });
}

function renderCharts(charts) {
    const container = document.getElementById('charts-section');
    container.innerHTML = '';
    
    charts.forEach(chart => {
        const chartContainer = document.createElement('div');
        chartContainer.className = 'chart-container bg-white rounded-lg shadow p-6';
        chartContainer.innerHTML = `
            <h4 class="text-lg font-semibold text-gray-900 mb-4">${chart.title}</h4>
            <div id="${chart.id}" style="height: 400px;"></div>
        `;
        container.appendChild(chartContainer);
        
        // Render the chart using Plotly
        setTimeout(() => {
            if (window.Plotly) {
                Plotly.newPlot(chart.id, chart.data.data, chart.data.layout, {responsive: true});
            }
        }, 100);
    });
}

function renderInsights(insights) {
    const container = document.getElementById('insights-container');
    container.innerHTML = '';
    
    if (insights.length === 0) {
        container.innerHTML = '<p class="text-gray-500 text-center py-4">No insights available</p>';
        return;
    }
    
    insights.forEach(insight => {
        const insightElement = document.createElement('div');
        insightElement.className = `insight-card ${insight.type} border-l-4 rounded-lg p-4 mb-4`;
        insightElement.innerHTML = `
            <div class="flex items-start">
                <i class="fas fa-${getInsightIcon(insight.type)} mt-1 mr-3"></i>
                <div>
                    <h4 class="font-semibold text-gray-900">${insight.title}</h4>
                    <p class="text-gray-700 mt-1">${insight.message}</p>
                </div>
            </div>
        `;
        container.appendChild(insightElement);
    });
}

function renderRawData(rawData) {
    const container = document.getElementById('raw-data-display');
    
    // Format and display raw data summary
    const summary = {
        analysis_type: rawData.analysis_type || 'unknown',
        file_info: rawData.file_info || {},
        summary: "Analysis completed successfully"
    };
    
    container.textContent = JSON.stringify(summary, null, 2);
}

function getIconClass(iconName) {
    const iconMap = {
        'database': 'database',
        'tasks': 'tasks',
        'check-circle': 'check-circle',
        'users': 'users',
        'chart-line': 'chart-line',
        'clock': 'clock',
        'project-diagram': 'project-diagram',
        'calendar-alt': 'calendar-alt',
        'brain': 'brain'
    };
    return iconMap[iconName] || 'info-circle';
}

function getInsightIcon(type) {
    const iconMap = {
        'success': 'check-circle',
        'warning': 'exclamation-triangle', 
        'info': 'info-circle',
        'error': 'times-circle'
    };
    return iconMap[type] || 'info-circle';
}
