// Main JavaScript for PM Analysis Dashboard

class DashboardManager {
    constructor() {
        this.currentFile = null;
        this.dashboardData = null;
        this.initializeEventListeners();
    }

    initializeEventListeners() {
        // File input handlers
        const fileInput = document.getElementById('file-input');
        const browseBtn = document.getElementById('browse-btn');
        const uploadBtn = document.getElementById('upload-btn');
        const dropZone = document.getElementById('drop-zone');

        // Browse button click
        browseBtn.addEventListener('click', () => {
            fileInput.click();
        });

        // File input change
        fileInput.addEventListener('change', (e) => {
            this.handleFileSelect(e.target.files[0]);
        });

        // Drag and drop handlers
        dropZone.addEventListener('dragover', (e) => {
            e.preventDefault();
            dropZone.classList.add('dragover');
        });

        dropZone.addEventListener('dragleave', () => {
            dropZone.classList.remove('dragover');
        });

        dropZone.addEventListener('drop', (e) => {
            e.preventDefault();
            dropZone.classList.remove('dragover');
            this.handleFileSelect(e.dataTransfer.files[0]);
        });

        // Upload button click
        uploadBtn.addEventListener('click', () => {
            this.uploadAndAnalyze();
        });

        // Action buttons
        document.getElementById('new-analysis-btn').addEventListener('click', () => {
            this.resetDashboard();
        });

        document.getElementById('export-btn').addEventListener('click', () => {
            this.exportReport();
        });

        document.getElementById('retry-btn').addEventListener('click', () => {
            this.resetDashboard();
        });
    }

    handleFileSelect(file) {
        if (!file) return;

        // Validate file type
        const allowedTypes = [
            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            'application/vnd.ms-excel',
            'text/csv',
            'application/json'
        ];

        if (!allowedTypes.includes(file.type) && !file.name.match(/\.(xlsx|xls|csv|json)$/i)) {
            this.showError('Invalid file type. Please upload Excel, CSV, or JSON files.');
            return;
        }

        // Check file size (16MB limit)
        if (file.size > 16 * 1024 * 1024) {
            this.showError('File too large. Maximum size is 16MB.');
            return;
        }

        this.currentFile = file;
        this.showFileInfo(file);
    }

    showFileInfo(file) {
        const fileInfo = document.getElementById('file-info');
        const fileName = document.getElementById('file-name');
        
        fileName.textContent = `${file.name} (${this.formatFileSize(file.size)})`;
        fileInfo.classList.remove('hidden');
    }

    formatFileSize(bytes) {
        if (bytes === 0) return '0 Bytes';
        const k = 1024;
        const sizes = ['Bytes', 'KB', 'MB', 'GB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
    }

    async uploadAndAnalyze() {
        if (!this.currentFile) return;

        this.showLoading();

        const formData = new FormData();
        formData.append('file', this.currentFile);

        try {
            const response = await fetch('/upload', {
                method: 'POST',
                body: formData
            });

            const result = await response.json();

            if (result.success) {
                this.dashboardData = result.data;
                this.renderDashboard();
            } else {
                this.showError(result.error || 'Analysis failed');
            }
        } catch (error) {
            this.showError(`Upload failed: ${error.message}`);
        }
    }

    showLoading() {
        document.getElementById('upload-section').classList.add('hidden');
        document.getElementById('loading-section').classList.remove('hidden');
        document.getElementById('dashboard-content').classList.add('hidden');
        document.getElementById('error-section').classList.add('hidden');
    }

    showError(message) {
        document.getElementById('upload-section').classList.remove('hidden');
        document.getElementById('loading-section').classList.add('hidden');
        document.getElementById('dashboard-content').classList.add('hidden');
        document.getElementById('error-section').classList.remove('hidden');
        document.getElementById('error-message').textContent = message;
    }

    renderDashboard() {
        document.getElementById('upload-section').classList.add('hidden');
        document.getElementById('loading-section').classList.add('hidden');
        document.getElementById('error-section').classList.add('hidden');
        document.getElementById('dashboard-content').classList.remove('hidden');

        this.renderSummaryCards();
        this.renderCharts();
        this.renderInsights();
        this.renderRawData();
    }

    renderSummaryCards() {
        const container = document.getElementById('summary-cards');
        container.innerHTML = '';

        const cards = this.dashboardData.summary_cards || [];
        
        cards.forEach(card => {
            const cardElement = this.createSummaryCard(card);
            container.appendChild(cardElement);
        });
    }

    createSummaryCard(card) {
        const div = document.createElement('div');
        div.className = 'summary-card bg-white rounded-lg shadow p-6';
        
        const iconClass = this.getIconClass(card.icon);
        
        div.innerHTML = `
            <div class="flex items-center">
                <div class="flex-shrink-0">
                    <i class="${iconClass} text-2xl text-blue-600"></i>
                </div>
                <div class="ml-4">
                    <p class="text-sm font-medium text-gray-500">${card.title}</p>
                    <p class="text-2xl font-bold text-gray-900">${card.value}</p>
                    <p class="text-sm text-gray-600">${card.subtitle}</p>
                </div>
            </div>
        `;
        
        return div;
    }

    getIconClass(iconName) {
        const iconMap = {
            'database': 'fas fa-database',
            'tasks': 'fas fa-tasks',
            'check-circle': 'fas fa-check-circle',
            'users': 'fas fa-users',
            'chart-line': 'fas fa-chart-line',
            'clock': 'fas fa-clock'
        };
        return iconMap[iconName] || 'fas fa-info-circle';
    }

    renderCharts() {
        const container = document.getElementById('charts-section');
        container.innerHTML = '';

        const charts = this.dashboardData.charts || [];
        
        charts.forEach(chart => {
            const chartContainer = this.createChartContainer(chart);
            container.appendChild(chartContainer);
            
            // Render the chart using Plotly
            setTimeout(() => {
                Plotly.newPlot(chart.id, chart.data.data, chart.data.layout, {responsive: true});
            }, 100);
        });
    }

    createChartContainer(chart) {
        const div = document.createElement('div');
        div.className = 'chart-container';
        div.innerHTML = `
            <h4 class="text-lg font-semibold text-gray-900 mb-4">${chart.title}</h4>
            <div id="${chart.id}" style="height: 400px;"></div>
        `;
        return div;
    }

    renderInsights() {
        const container = document.getElementById('insights-container');
        container.innerHTML = '';

        const insights = this.dashboardData.insights || [];
        
        if (insights.length === 0) {
            container.innerHTML = '<p class="text-gray-500 text-center py-4">No insights available</p>';
            return;
        }

        insights.forEach(insight => {
            const insightElement = this.createInsightCard(insight);
            container.appendChild(insightElement);
        });
    }

    createInsightCard(insight) {
        const div = document.createElement('div');
        div.className = `insight-card ${insight.type}`;
        
        const iconClass = this.getInsightIcon(insight.type);
        
        div.innerHTML = `
            <div class="flex items-start">
                <i class="${iconClass} mt-1 mr-3"></i>
                <div>
                    <h4 class="font-semibold text-gray-900">${insight.title}</h4>
                    <p class="text-gray-700 mt-1">${insight.message}</p>
                </div>
            </div>
        `;
        
        return div;
    }

    getInsightIcon(type) {
        const iconMap = {
            'success': 'fas fa-check-circle text-green-600',
            'warning': 'fas fa-exclamation-triangle text-yellow-600',
            'info': 'fas fa-info-circle text-blue-600',
            'error': 'fas fa-times-circle text-red-600'
        };
        return iconMap[type] || 'fas fa-info-circle text-blue-600';
    }

    renderRawData() {
        const container = document.getElementById('raw-data-display');
        const rawData = this.dashboardData.raw_data || {};
        
        // Format and display raw data summary
        const summary = {
            file_info: rawData.file_info || {},
            task_summary: {
                total_tasks: rawData.task_analysis?.total_tasks || 0,
                status_breakdown: rawData.task_analysis?.status_breakdown || {},
                team_size: rawData.team_analysis?.team_size || 0
            }
        };
        
        container.textContent = JSON.stringify(summary, null, 2);
    }

    exportReport() {
        if (!this.dashboardData) return;

        const reportData = {
            timestamp: new Date().toISOString(),
            file_analyzed: this.currentFile?.name || 'Unknown',
            summary: this.dashboardData.summary_cards,
            insights: this.dashboardData.insights,
            raw_analysis: this.dashboardData.raw_data
        };

        const dataStr = JSON.stringify(reportData, null, 2);
        const dataBlob = new Blob([dataStr], {type: 'application/json'});
        
        const url = URL.createObjectURL(dataBlob);
        const link = document.createElement('a');
        link.href = url;
        link.download = `pm_analysis_report_${new Date().toISOString().split('T')[0]}.json`;
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        URL.revokeObjectURL(url);
    }

    resetDashboard() {
        this.currentFile = null;
        this.dashboardData = null;
        
        document.getElementById('upload-section').classList.remove('hidden');
        document.getElementById('loading-section').classList.add('hidden');
        document.getElementById('dashboard-content').classList.add('hidden');
        document.getElementById('error-section').classList.add('hidden');
        document.getElementById('file-info').classList.add('hidden');
        
        document.getElementById('file-input').value = '';
    }
}

// Initialize dashboard when page loads
document.addEventListener('DOMContentLoaded', () => {
    new DashboardManager();
});
