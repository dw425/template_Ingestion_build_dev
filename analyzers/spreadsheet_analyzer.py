import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json

class SpreadsheetAnalyzer:
    def __init__(self):
        self.data = None
        self.analysis_results = {}
    
    def analyze_file(self, filepath):
        """Analyze uploaded spreadsheet file"""
        try:
            # Read file based on extension
            if filepath.endswith('.csv'):
                self.data = pd.read_csv(filepath)
            elif filepath.endswith('.json'):
                # Handle JSON files
                with open(filepath, 'r', encoding='utf-8') as f:
                    json_data = json.load(f)
                
                # Convert JSON to DataFrame
                if isinstance(json_data, list):
                    # If JSON is a list of objects
                    self.data = pd.DataFrame(json_data)
                elif isinstance(json_data, dict):
                    # Check if it's a nested structure with data arrays
                    if 'issues' in json_data:  # JIRA export format
                        self.data = pd.DataFrame(json_data['issues'])
                    elif 'data' in json_data:  # Generic data wrapper
                        self.data = pd.DataFrame(json_data['data'])
                    else:
                        # Try to normalize the dictionary
                        self.data = pd.json_normalize(json_data)
                else:
                    raise Exception("JSON format not supported - must be list of objects or dictionary")
                    
            else:  # xlsx, xls
                self.data = pd.read_excel(filepath)
            
            # Ensure we have data
            if self.data.empty:
                raise Exception("No data found in file")
            
            # Perform comprehensive analysis
            self.analysis_results = {
                'file_info': self._get_file_info(),
                'data_summary': self._get_data_summary(),
                'task_analysis': self._analyze_tasks(),
                'timeline_analysis': self._analyze_timeline(),
                'completion_analysis': self._analyze_completion(),
                'team_analysis': self._analyze_team_performance()
            }
            
            return self.analysis_results
            
        except Exception as e:
            raise Exception(f"Error analyzing file: {str(e)}")
    
    def _get_file_info(self):
        """Get basic file information"""
        return {
            'total_rows': len(self.data),
            'total_columns': len(self.data.columns),
            'columns': list(self.data.columns),
            'data_types': self.data.dtypes.astype(str).to_dict()
        }
    
    def _get_data_summary(self):
        """Get summary statistics"""
        numeric_cols = self.data.select_dtypes(include=[np.number]).columns
        summary = {}
        
        for col in numeric_cols:
            summary[col] = {
                'mean': float(self.data[col].mean()) if not pd.isna(self.data[col].mean()) else None,
                'median': float(self.data[col].median()) if not pd.isna(self.data[col].median()) else None,
                'std': float(self.data[col].std()) if not pd.isna(self.data[col].std()) else None,
                'min': float(self.data[col].min()) if not pd.isna(self.data[col].min()) else None,
                'max': float(self.data[col].max()) if not pd.isna(self.data[col].max()) else None
            }
        
        return summary
    
    def _analyze_tasks(self):
        """Analyze task-related data"""
        task_analysis = {}
        
        # Look for common task-related column names
        task_columns = self._find_columns(['task', 'title', 'summary', 'description', 'issue', 'key', 'subject'])
        status_columns = self._find_columns(['status', 'state', 'progress', 'resolution'])
        priority_columns = self._find_columns(['priority', 'severity', 'importance'])
        
        if task_columns:
            task_col = task_columns[0]
            task_analysis['total_tasks'] = len(self.data)
            task_analysis['unique_tasks'] = self.data[task_col].nunique()
        
        if status_columns:
            status_col = status_columns[0]
            status_counts = self.data[status_col].value_counts().to_dict()
            task_analysis['status_breakdown'] = status_counts
        
        if priority_columns:
            priority_col = priority_columns[0]
            priority_counts = self.data[priority_col].value_counts().to_dict()
            task_analysis['priority_breakdown'] = priority_counts
        
        return task_analysis
    
    def _analyze_timeline(self):
        """Analyze timeline and date-related data"""
        timeline_analysis = {}
        
        # Look for date columns
        date_columns = self._find_date_columns()
        
        if date_columns:
            for date_col in date_columns[:2]:  # Analyze up to 2 date columns
                try:
                    dates = pd.to_datetime(self.data[date_col], errors='coerce')
                    valid_dates = dates.dropna()
                    
                    if len(valid_dates) > 0:
                        timeline_analysis[f'{date_col}_analysis'] = {
                            'earliest': valid_dates.min().isoformat(),
                            'latest': valid_dates.max().isoformat(),
                            'span_days': (valid_dates.max() - valid_dates.min()).days,
                            'valid_dates': len(valid_dates),
                            'missing_dates': len(dates) - len(valid_dates)
                        }
                except Exception:
                    continue
        
        return timeline_analysis
    
    def _analyze_completion(self):
        """Analyze completion rates and progress"""
        completion_analysis = {}
        
        # Look for completion indicators
        completion_columns = self._find_columns(['complete', 'done', 'finished', 'resolved', 'closed'])
        progress_columns = self._find_columns(['progress', 'percent', '%'])
        
        if completion_columns:
            for col in completion_columns:
                if self.data[col].dtype == 'bool' or self.data[col].dtype == 'object':
                    completion_rate = self._calculate_completion_rate(self.data[col])
                    completion_analysis[f'{col}_rate'] = completion_rate
        
        if progress_columns:
            for col in progress_columns:
                if pd.api.types.is_numeric_dtype(self.data[col]):
                    avg_progress = float(self.data[col].mean()) if not pd.isna(self.data[col].mean()) else 0
                    completion_analysis[f'{col}_average'] = avg_progress
        
        return completion_analysis
    
    def _analyze_team_performance(self):
        """Analyze team member performance"""
        team_analysis = {}
        
        # Look for assignee/team member columns
        assignee_columns = self._find_columns(['assignee', 'assigned', 'owner', 'responsible', 'team', 'reporter', 'creator'])
        
        if assignee_columns:
            assignee_col = assignee_columns[0]
            team_counts = self.data[assignee_col].value_counts().to_dict()
            team_analysis['task_distribution'] = team_counts
            team_analysis['team_size'] = self.data[assignee_col].nunique()
        
        return team_analysis
    
    def _find_columns(self, keywords):
        """Find columns that contain any of the given keywords"""
        found_columns = []
        for col in self.data.columns:
            col_lower = str(col).lower()
            if any(keyword.lower() in col_lower for keyword in keywords):
                found_columns.append(col)
        return found_columns
    
    def _find_date_columns(self):
        """Find columns that likely contain dates"""
        date_columns = []
        for col in self.data.columns:
            col_lower = str(col).lower()
            date_keywords = ['date', 'time', 'created', 'updated', 'due', 'start', 'end', 'deadline']
            if any(keyword in col_lower for keyword in date_keywords):
                date_columns.append(col)
        return date_columns
    
    def _calculate_completion_rate(self, series):
        """Calculate completion rate for a boolean or categorical series"""
        if series.dtype == 'bool':
            return float(series.sum() / len(series)) * 100
        else:
            # Look for completion indicators in text
            completion_indicators = ['done', 'complete', 'finished', 'resolved', 'closed', 'yes', 'true']
            completed = series.astype(str).str.lower().isin(completion_indicators)
            return float(completed.sum() / len(series)) * 100
