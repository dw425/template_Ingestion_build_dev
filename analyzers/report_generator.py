import plotly.graph_objs as go
import plotly.utils
import json
from datetime import datetime

class ReportGenerator:
    def __init__(self):
        pass
    
    def generate_dashboard_data(self, analysis_results, analysis_type="spreadsheet"):
        """Generate dashboard data from analysis results"""
        
        if analysis_type == "document":
            return self._generate_document_dashboard_data(analysis_results)
        else:
            return self._generate_spreadsheet_dashboard_data(analysis_results)
    
    def _generate_spreadsheet_dashboard_data(self, analysis_results):
        """Generate dashboard data for spreadsheet analysis"""
        dashboard_data = {
            'summary_cards': self._create_summary_cards(analysis_results),
            'charts': self._create_charts(analysis_results),
            'insights': self._generate_insights(analysis_results),
            'raw_data': analysis_results,
            'analysis_type': 'spreadsheet'
        }
        
        return dashboard_data
    
    def _generate_document_dashboard_data(self, analysis_results):
        """Generate dashboard data for AI document analysis"""
        ai_analysis = analysis_results.get('ai_analysis', {})
        
        dashboard_data = {
            'summary_cards': self._create_document_summary_cards(ai_analysis),
            'charts': self._create_document_charts(ai_analysis),
            'insights': self._generate_document_insights(ai_analysis),
            'raw_data': analysis_results,
            'analysis_type': 'document',
            'ai_analysis': ai_analysis
        }
        
        return dashboard_data
    
    def _create_summary_cards(self, analysis_results):
        """Create summary cards for the dashboard"""
        cards = []
        
        # File info card
        file_info = analysis_results.get('file_info', {})
        cards.append({
            'title': 'Data Overview',
            'value': f"{file_info.get('total_rows', 0)} rows",
            'subtitle': f"{file_info.get('total_columns', 0)} columns",
            'icon': 'database'
        })
        
        # Task analysis card
        task_analysis = analysis_results.get('task_analysis', {})
        if 'total_tasks' in task_analysis:
            cards.append({
                'title': 'Total Tasks',
                'value': str(task_analysis.get('total_tasks', 0)),
                'subtitle': f"{task_analysis.get('unique_tasks', 0)} unique",
                'icon': 'tasks'
            })
        
        # Completion rate card
        completion_analysis = analysis_results.get('completion_analysis', {})
        if completion_analysis:
            # Find the first completion rate
            for key, value in completion_analysis.items():
                if 'rate' in key:
                    cards.append({
                        'title': 'Completion Rate',
                        'value': f"{value:.1f}%",
                        'subtitle': 'Overall progress',
                        'icon': 'check-circle'
                    })
                    break
        
        # Team size card
        team_analysis = analysis_results.get('team_analysis', {})
        if 'team_size' in team_analysis:
            cards.append({
                'title': 'Team Members',
                'value': str(team_analysis.get('team_size', 0)),
                'subtitle': 'Active contributors',
                'icon': 'users'
            })
        
        return cards
    
    def _create_charts(self, analysis_results):
        """Create charts for the dashboard"""
        charts = []
        
        # Status breakdown pie chart
        task_analysis = analysis_results.get('task_analysis', {})
        if 'status_breakdown' in task_analysis:
            status_data = task_analysis['status_breakdown']
            
            fig = go.Figure(data=[go.Pie(
                labels=list(status_data.keys()),
                values=list(status_data.values()),
                hole=.3
            )])
            
            fig.update_layout(
                title="Task Status Distribution",
                font=dict(size=12),
                height=400
            )
            
            charts.append({
                'id': 'status_chart',
                'title': 'Task Status Distribution',
                'type': 'pie',
                'data': json.loads(plotly.utils.PlotlyJSONEncoder().encode(fig))
            })
        
        # Priority breakdown bar chart
        if 'priority_breakdown' in task_analysis:
            priority_data = task_analysis['priority_breakdown']
            
            fig = go.Figure(data=[go.Bar(
                x=list(priority_data.keys()),
                y=list(priority_data.values()),
                marker_color='lightblue'
            )])
            
            fig.update_layout(
                title="Task Priority Distribution",
                xaxis_title="Priority Level",
                yaxis_title="Number of Tasks",
                font=dict(size=12),
                height=400
            )
            
            charts.append({
                'id': 'priority_chart',
                'title': 'Task Priority Distribution',
                'type': 'bar',
                'data': json.loads(plotly.utils.PlotlyJSONEncoder().encode(fig))
            })
        
        # Team workload chart
        team_analysis = analysis_results.get('team_analysis', {})
        if 'task_distribution' in team_analysis:
            team_data = team_analysis['task_distribution']
            
            # Get top 10 team members by task count
            sorted_team = sorted(team_data.items(), key=lambda x: x[1], reverse=True)[:10]
            
            fig = go.Figure(data=[go.Bar(
                x=[item[1] for item in sorted_team],
                y=[item[0] for item in sorted_team],
                orientation='h',
                marker_color='lightgreen'
            )])
            
            fig.update_layout(
                title="Team Workload Distribution",
                xaxis_title="Number of Tasks",
                yaxis_title="Team Member",
                font=dict(size=12),
                height=500
            )
            
            charts.append({
                'id': 'team_chart',
                'title': 'Team Workload Distribution',
                'type': 'bar',
                'data': json.loads(plotly.utils.PlotlyJSONEncoder().encode(fig))
            })
        
        return charts
    
    def _generate_insights(self, analysis_results):
        """Generate insights from the analysis"""
        insights = []
        
        # File insights
        file_info = analysis_results.get('file_info', {})
        insights.append({
            'type': 'info',
            'title': 'Data Quality',
            'message': f"Successfully processed {file_info.get('total_rows', 0)} records with {file_info.get('total_columns', 0)} attributes."
        })
        
        # Task completion insights
        task_analysis = analysis_results.get('task_analysis', {})
        if 'status_breakdown' in task_analysis:
            status_data = task_analysis['status_breakdown']
            total_tasks = sum(status_data.values())
            
            # Look for completion indicators
            completed_keys = [k for k in status_data.keys() if any(word in str(k).lower() 
                            for word in ['done', 'complete', 'finished', 'resolved', 'closed'])]
            
            if completed_keys:
                completed_count = sum(status_data[k] for k in completed_keys)
                completion_rate = (completed_count / total_tasks) * 100
                
                if completion_rate > 80:
                    insights.append({
                        'type': 'success',
                        'title': 'High Completion Rate',
                        'message': f"Excellent progress! {completion_rate:.1f}% of tasks are completed."
                    })
                elif completion_rate < 50:
                    insights.append({
                        'type': 'warning',
                        'title': 'Low Completion Rate',
                        'message': f"Only {completion_rate:.1f}% of tasks are completed. Consider reviewing priorities."
                    })
        
        # Team balance insights
        team_analysis = analysis_results.get('team_analysis', {})
        if 'task_distribution' in team_analysis:
            team_data = team_analysis['task_distribution']
            task_counts = list(team_data.values())
            
            if len(task_counts) > 1:
                max_tasks = max(task_counts)
                min_tasks = min(task_counts)
                
                if max_tasks > min_tasks * 3:  # If imbalance is significant
                    insights.append({
                        'type': 'warning',
                        'title': 'Workload Imbalance',
                        'message': f"Significant workload variation detected. Range: {min_tasks}-{max_tasks} tasks per person."
                    })
                else:
                    insights.append({
                        'type': 'success',
                        'title': 'Balanced Workload',
                        'message': "Tasks are fairly distributed across team members."
                    })
        
        # Timeline insights
        timeline_analysis = analysis_results.get('timeline_analysis', {})
        for key, timeline_data in timeline_analysis.items():
            if isinstance(timeline_data, dict) and 'span_days' in timeline_data:
                span_days = timeline_data['span_days']
                if span_days > 365:
                    insights.append({
                        'type': 'info',
                        'title': 'Long-term Project',
                        'message': f"Project spans {span_days} days. Consider milestone tracking."
                    })
                elif span_days < 30:
                    insights.append({
                        'type': 'info',
                        'title': 'Short-term Project',
                        'message': f"Project duration: {span_days} days. Fast execution timeline."
                    })
        
        return insights
    
    def _create_document_summary_cards(self, ai_analysis):
        """Create summary cards for document analysis"""
        cards = []
        
        # Project overview card
        project_overview = ai_analysis.get('project_overview', {})
        cards.append({
            'title': 'Project Name',
            'value': project_overview.get('project_name', 'Unknown')[:20],
            'subtitle': project_overview.get('project_type', 'Unknown Type'),
            'icon': 'project-diagram'
        })
        
        # Timeline card
        timeline_info = ai_analysis.get('timeline_info', {})
        milestones_count = len(timeline_info.get('milestones', []))
        cards.append({
            'title': 'Milestones',
            'value': str(milestones_count),
            'subtitle': 'Key milestones identified',
            'icon': 'calendar-alt'
        })
        
        # Team card
        team_info = ai_analysis.get('team_and_resources', {})
        team_size = len(team_info.get('team_members', []))
        cards.append({
            'title': 'Team Members',
            'value': str(team_size),
            'subtitle': 'People identified',
            'icon': 'users'
        })
        
        # Confidence card
        metadata = ai_analysis.get('document_metadata', {})
        confidence = metadata.get('confidence_score', 0)
        cards.append({
            'title': 'AI Confidence',
            'value': f"{confidence * 100:.0f}%",
            'subtitle': 'Analysis accuracy',
            'icon': 'brain'
        })
        
        return cards
    
    def _create_document_charts(self, ai_analysis):
        """Create charts for document analysis"""
        charts = []
        
        # Status breakdown
        status_info = ai_analysis.get('status_and_progress', {})
        completed = len(status_info.get('completed_tasks', []))
        in_progress = len(status_info.get('in_progress_tasks', []))
        pending = len(status_info.get('pending_tasks', []))
        
        if completed + in_progress + pending > 0:
            fig = go.Figure(data=[go.Pie(
                labels=['Completed', 'In Progress', 'Pending'],
                values=[completed, in_progress, pending],
                hole=.3,
                marker_colors=['#10B981', '#F59E0B', '#EF4444']
            )])
            
            fig.update_layout(
                title="Project Task Status",
                font=dict(size=12),
                height=400
            )
            
            charts.append({
                'id': 'document_status_chart',
                'title': 'Task Status Distribution',
                'type': 'pie',
                'data': json.loads(plotly.utils.PlotlyJSONEncoder().encode(fig))
            })
        
        # Team workload
        team_info = ai_analysis.get('team_and_resources', {})
        team_members = team_info.get('team_members', [])
        
        if team_members:
            # Create a simple bar chart of team members
            fig = go.Figure(data=[go.Bar(
                x=team_members[:10],  # Limit to 10 members
                y=[1] * len(team_members[:10]),  # Each person counts as 1
                marker_color='lightblue'
            )])
            
            fig.update_layout(
                title="Team Members Identified",
                xaxis_title="Team Member",
                yaxis_title="Mentions",
                font=dict(size=12),
                height=400
            )
            
            charts.append({
                'id': 'document_team_chart',
                'title': 'Team Members',
                'type': 'bar',
                'data': json.loads(plotly.utils.PlotlyJSONEncoder().encode(fig))
            })
        
        # KPIs and Metrics
        kpis = ai_analysis.get('kpis_and_metrics', {})
        metrics = kpis.get('performance_metrics', [])
        
        if metrics:
            # Create a horizontal bar chart of metrics
            metric_names = [metric.split(':')[0] if ':' in metric else metric for metric in metrics[:8]]
            metric_counts = [1] * len(metric_names)  # Each metric counts as 1
            
            fig = go.Figure(data=[go.Bar(
                x=metric_counts,
                y=metric_names,
                orientation='h',
                marker_color='lightgreen'
            )])
            
            fig.update_layout(
                title="Key Performance Indicators",
                xaxis_title="Identified",
                yaxis_title="KPI Type",
                font=dict(size=12),
                height=500
            )
            
            charts.append({
                'id': 'document_kpi_chart',
                'title': 'Key Performance Indicators',
                'type': 'bar',
                'data': json.loads(plotly.utils.PlotlyJSONEncoder().encode(fig))
            })
        
        return charts
    
    def _generate_document_insights(self, ai_analysis):
        """Generate insights for document analysis"""
        insights = []
        
        # Document quality insight
        metadata = ai_analysis.get('document_metadata', {})
        confidence = metadata.get('confidence_score', 0)
        data_quality = metadata.get('data_quality', '')
        
        if confidence > 0.8:
            insights.append({
                'type': 'success',
                'title': 'High Quality Analysis',
                'message': f"AI analysis achieved {confidence*100:.0f}% confidence. {data_quality}"
            })
        elif confidence > 0.5:
            insights.append({
                'type': 'info',
                'title': 'Moderate Analysis Quality',
                'message': f"AI analysis achieved {confidence*100:.0f}% confidence. Some information may be incomplete."
            })
        else:
            insights.append({
                'type': 'warning',
                'title': 'Limited Analysis Quality',
                'message': f"AI analysis achieved only {confidence*100:.0f}% confidence. Consider uploading a clearer document."
            })
        
        # Project timeline insights
        timeline_info = ai_analysis.get('timeline_info', {})
        start_date = timeline_info.get('start_date')
        end_date = timeline_info.get('end_date')
        milestones = timeline_info.get('milestones', [])
        
        if start_date and end_date:
            insights.append({
                'type': 'info',
                'title': 'Project Timeline Identified',
                'message': f"Project spans from {start_date} to {end_date} with {len(milestones)} milestones."
            })
        elif milestones:
            insights.append({
                'type': 'info',
                'title': 'Milestones Found',
                'message': f"Identified {len(milestones)} key milestones without clear start/end dates."
            })
        
        # Team and resources insights
        team_info = ai_analysis.get('team_and_resources', {})
        team_members = team_info.get('team_members', [])
        budget_info = team_info.get('budget_info', '')
        
        if len(team_members) > 10:
            insights.append({
                'type': 'info',
                'title': 'Large Team Project',
                'message': f"Large team identified with {len(team_members)} members. Consider coordination strategies."
            })
        elif len(team_members) < 3:
            insights.append({
                'type': 'warning',
                'title': 'Small Team',
                'message': f"Only {len(team_members)} team members identified. May need additional resources."
            })
        
        if budget_info and budget_info != "Not available":
            insights.append({
                'type': 'success',
                'title': 'Budget Information Available',
                'message': f"Budget details found: {budget_info[:100]}..."
            })
        
        # Status and progress insights
        status_info = ai_analysis.get('status_and_progress', {})
        completed_tasks = status_info.get('completed_tasks', [])
        pending_tasks = status_info.get('pending_tasks', [])
        risks_issues = status_info.get('risks_issues', [])
        
        total_tasks = len(completed_tasks) + len(status_info.get('in_progress_tasks', [])) + len(pending_tasks)
        if total_tasks > 0:
            completion_rate = (len(completed_tasks) / total_tasks) * 100
            
            if completion_rate > 80:
                insights.append({
                    'type': 'success',
                    'title': 'High Progress Rate',
                    'message': f"Project shows strong progress with {completion_rate:.1f}% of tasks completed."
                })
            elif completion_rate < 30:
                insights.append({
                    'type': 'warning',
                    'title': 'Low Progress Rate',
                    'message': f"Only {completion_rate:.1f}% of tasks completed. Consider acceleration strategies."
                })
        
        if risks_issues:
            insights.append({
                'type': 'warning',
                'title': 'Risks Identified',
                'message': f"Found {len(risks_issues)} potential risks or issues requiring attention."
            })
        
        # Key insights from AI analysis
        key_insights = ai_analysis.get('key_insights', {})
        concerns = key_insights.get('concerns', [])
        recommendations = key_insights.get('recommendations', [])
        
        if recommendations:
            insights.append({
                'type': 'info',
                'title': 'AI Recommendations',
                'message': f"AI identified {len(recommendations)} recommendations for improvement."
            })
        
        if concerns:
            insights.append({
                'type': 'warning',
                'title': 'Areas of Concern',
                'message': f"AI flagged {len(concerns)} areas requiring attention."
            })
        
        # Missing information insights
        missing_info = metadata.get('missing_information', [])
        if missing_info:
            insights.append({
                'type': 'info',
                'title': 'Information Gaps',
                'message': f"Consider adding information about: {', '.join(missing_info[:3])}"
            })
        
        return insights
