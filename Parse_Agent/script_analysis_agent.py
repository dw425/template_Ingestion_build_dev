# Agent #002 - post ingestion agent
"""
NiFi Script Analysis Agent - Script Collection & Analysis
==========================================================

This agent extracts and analyzes external scripts referenced by NiFi ExecuteStreamCommand
processors to understand the actual data operations (Impala, Kudu, HDFS, etc.)

Author: Dan Warren
Version: 1.0
Agent Series: #000-1200-002
"""

import json
import os
import re
import sys
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional
from collections import defaultdict
from datetime import datetime

# ==============================================================================
# CONFIGURATION
# ==============================================================================

# Default paths
DEFAULT_CONTRACT_PATH = "data_contract.json"
DEFAULT_OUTPUT_DIR = "script_analysis"

# Script file extensions to look for
SCRIPT_EXTENSIONS = ['.sh', '.bash', '.py', '.sql', '.hql', '.R', '.scala']

# Common script directories in NiFi deployments
COMMON_SCRIPT_PATHS = [
    '/opt/nifi/scripts',
    '/nifi/scripts',
    '${NIFI_HOME}/scripts',
    './scripts',
    '../scripts'
]

# ==============================================================================
# DATABASE OPERATION PATTERNS
# ==============================================================================

# Patterns to identify database operations in scripts
IMPALA_PATTERNS = [
    r'impala-shell',
    r'beeline.*hive',
    r'INVALIDATE\s+METADATA',
    r'COMPUTE\s+STATS',
    r'CREATE\s+TABLE',
    r'INSERT\s+INTO',
    r'SELECT\s+.*\s+FROM',
    r'UPDATE\s+.*\s+SET',
    r'DELETE\s+FROM',
    r'TRUNCATE\s+TABLE',
    r'ALTER\s+TABLE',
    r'DROP\s+TABLE',
    r'REFRESH\s+.*',
    r'MERGE\s+INTO'
]

KUDU_PATTERNS = [
    r'kudu\s+table',
    r'kudu\s+scan',
    r'kudu\s+insert',
    r'kudu\s+update',
    r'kudu\s+delete',
    r'kudu\s+create',
    r'kudu\s+alter',
    r'CREATE\s+TABLE.*STORED\s+AS\s+KUDU',
    r'CREATE\s+EXTERNAL\s+TABLE.*KUDU',
    r'UPSERT\s+INTO'
]

HDFS_PATTERNS = [
    r'hdfs\s+dfs\s+-put',
    r'hdfs\s+dfs\s+-get',
    r'hdfs\s+dfs\s+-mv',
    r'hdfs\s+dfs\s+-cp',
    r'hdfs\s+dfs\s+-rm',
    r'hdfs\s+dfs\s+-mkdir',
    r'hdfs\s+dfs\s+-ls',
    r'hdfs\s+dfs\s+-cat',
    r'hadoop\s+fs\s+-'
]

HIVE_PATTERNS = [
    r'hive\s+-e',
    r'hive\s+-f',
    r'beeline\s+-u',
    r'SET\s+hive\.',
    r'MSCK\s+REPAIR\s+TABLE'
]

# Table name patterns
TABLE_NAME_PATTERN = r'(?:FROM|INTO|TABLE|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*|[a-zA-Z_][a-zA-Z0-9_]*)'

# Connection string patterns
CONNECTION_PATTERNS = [
    r'jdbc:impala://([^:]+):(\d+)/([^;"\'\s]+)',
    r'jdbc:hive2://([^:]+):(\d+)/([^;"\'\s]+)',
    r'-h\s+([a-zA-Z0-9\-\.]+)',  # Host flag
    r'--host[=\s]+([a-zA-Z0-9\-\.]+)',
    r'IMPALAD_HOST[=\s]*([a-zA-Z0-9\-\.]+)'
]

# ==============================================================================
# SCRIPT PROCESSOR CLASS
# ==============================================================================

class ScriptProcessor:
    """Analyzes individual script files for database operations"""
    
    def __init__(self, script_path: str, content: str):
        self.path = script_path
        self.content = content
        self.name = os.path.basename(script_path)
        self.extension = os.path.splitext(script_path)[1]
        
    def analyze(self) -> Dict:
        """Perform comprehensive analysis of the script"""
        
        analysis = {
            'script_path': self.path,
            'script_name': self.name,
            'file_type': self.extension,
            'line_count': len(self.content.splitlines()),
            'operations': self._identify_operations(),
            'tables': self._extract_tables(),
            'connections': self._extract_connections(),
            'dependencies': self._extract_dependencies(),
            'complexity': self._assess_complexity()
        }
        
        return analysis
    
    def _identify_operations(self) -> Dict:
        """Identify database and storage operations"""
        
        operations = {
            'impala': [],
            'kudu': [],
            'hdfs': [],
            'hive': []
        }
        
        # Check for Impala operations
        for pattern in IMPALA_PATTERNS:
            matches = re.finditer(pattern, self.content, re.IGNORECASE | re.MULTILINE)
            for match in matches:
                line_num = self.content[:match.start()].count('\n') + 1
                operations['impala'].append({
                    'line': line_num,
                    'operation': match.group(0),
                    'context': self._get_context(match.start())
                })
        
        # Check for Kudu operations
        for pattern in KUDU_PATTERNS:
            matches = re.finditer(pattern, self.content, re.IGNORECASE | re.MULTILINE)
            for match in matches:
                line_num = self.content[:match.start()].count('\n') + 1
                operations['kudu'].append({
                    'line': line_num,
                    'operation': match.group(0),
                    'context': self._get_context(match.start())
                })
        
        # Check for HDFS operations
        for pattern in HDFS_PATTERNS:
            matches = re.finditer(pattern, self.content, re.IGNORECASE | re.MULTILINE)
            for match in matches:
                line_num = self.content[:match.start()].count('\n') + 1
                operations['hdfs'].append({
                    'line': line_num,
                    'operation': match.group(0),
                    'context': self._get_context(match.start())
                })
        
        # Check for Hive operations
        for pattern in HIVE_PATTERNS:
            matches = re.finditer(pattern, self.content, re.IGNORECASE | re.MULTILINE)
            for match in matches:
                line_num = self.content[:match.start()].count('\n') + 1
                operations['hive'].append({
                    'line': line_num,
                    'operation': match.group(0),
                    'context': self._get_context(match.start())
                })
        
        return operations
    
    def _extract_tables(self) -> List[Dict]:
        """Extract table references from SQL statements"""
        
        tables = []
        matches = re.finditer(TABLE_NAME_PATTERN, self.content, re.IGNORECASE)
        
        for match in matches:
            table_name = match.group(1)
            line_num = self.content[:match.start()].count('\n') + 1
            
            tables.append({
                'table': table_name,
                'line': line_num,
                'context': self._get_context(match.start())
            })
        
        return tables
    
    def _extract_connections(self) -> List[Dict]:
        """Extract database connection details"""
        
        connections = []
        
        for pattern in CONNECTION_PATTERNS:
            matches = re.finditer(pattern, self.content, re.IGNORECASE)
            for match in matches:
                line_num = self.content[:match.start()].count('\n') + 1
                connections.append({
                    'line': line_num,
                    'connection_string': match.group(0),
                    'groups': match.groups()
                })
        
        return connections
    
    def _extract_dependencies(self) -> List[str]:
        """Extract script dependencies (sourced files, imports)"""
        
        dependencies = []
        
        # Shell script sourcing
        source_pattern = r'(?:source|\.)[ \t]+([^\s;]+)'
        for match in re.finditer(source_pattern, self.content):
            dependencies.append(match.group(1))
        
        # Python imports from local files
        import_pattern = r'from\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+import|import\s+([a-zA-Z_][a-zA-Z0-9_]*)'
        for match in re.finditer(import_pattern, self.content):
            module = match.group(1) or match.group(2)
            if not module.startswith('sys') and not module.startswith('os'):
                dependencies.append(module)
        
        return list(set(dependencies))
    
    def _assess_complexity(self) -> str:
        """Assess script complexity"""
        
        line_count = len(self.content.splitlines())
        
        # Count loops, conditionals, functions
        control_structures = len(re.findall(r'\b(if|for|while|case|function)\b', self.content))
        
        if line_count > 200 or control_structures > 20:
            return "HIGH"
        elif line_count > 100 or control_structures > 10:
            return "MEDIUM"
        else:
            return "LOW"
    
    def _get_context(self, position: int, context_lines: int = 2) -> str:
        """Get context around a match position"""
        
        lines = self.content.splitlines()
        line_num = self.content[:position].count('\n')
        
        start = max(0, line_num - context_lines)
        end = min(len(lines), line_num + context_lines + 1)
        
        context = '\n'.join(lines[start:end])
        return context[:200] + '...' if len(context) > 200 else context

# === PAUSED HERE - CHUNK 1 ===
# === PAUSED HERE - CHUNK 1 ===

# ==============================================================================
# SCRIPT INVENTORY BUILDER
# ==============================================================================

class ScriptInventoryBuilder:
    """Extracts script references from NiFi data contract"""
    
    def __init__(self, contract_path: str):
        self.contract_path = contract_path
        self.contract = self._load_contract()
        
    def _load_contract(self) -> Dict:
        """Load the data contract JSON"""
        with open(self.contract_path, 'r') as f:
            return json.load(f)
    
    def build_inventory(self) -> Dict:
        """Extract all script references from ExecuteStreamCommand processors"""
        
        print("📋 Building script inventory from data contract...")
        
        inventory = {
            'total_script_processors': 0,
            'script_references': [],
            'command_patterns': defaultdict(int),
            'working_directories': set(),
            'environment_variables': set(),
            'summary': {}
        }
        
        processors = self.contract.get('components', {}).get('processors', [])
        
        for proc in processors:
            component = proc.get('component', {})
            proc_type = component.get('type', '')
            
            # Focus on ExecuteStreamCommand processors
            if 'ExecuteStreamCommand' in proc_type:
                inventory['total_script_processors'] += 1
                
                script_info = self._extract_script_info(proc, component)
                if script_info:
                    inventory['script_references'].append(script_info)
                    
                    # Track patterns
                    if script_info.get('command'):
                        cmd_base = script_info['command'].split()[0]
                        inventory['command_patterns'][cmd_base] += 1
                    
                    if script_info.get('working_directory'):
                        inventory['working_directories'].add(script_info['working_directory'])
                    
                    # Track environment variables
                    for env_var in script_info.get('environment_variables', []):
                        inventory['environment_variables'].add(env_var)
        
        # Convert sets to lists for JSON serialization
        inventory['working_directories'] = sorted(list(inventory['working_directories']))
        inventory['environment_variables'] = sorted(list(inventory['environment_variables']))
        inventory['command_patterns'] = dict(inventory['command_patterns'])
        
        # Generate summary
        inventory['summary'] = {
            'unique_scripts': len(set(s['command'] for s in inventory['script_references'] if s.get('command'))),
            'most_common_commands': sorted(inventory['command_patterns'].items(), key=lambda x: x[1], reverse=True)[:10]
        }
        
        print(f"   Found {inventory['total_script_processors']} ExecuteStreamCommand processors")
        print(f"   Identified {inventory['summary']['unique_scripts']} unique script commands")
        
        return inventory
    
    def _extract_script_info(self, proc: Dict, component: Dict) -> Optional[Dict]:
        """Extract script information from a processor"""
        
        # Get properties from both possible locations
        props = component.get('properties', {})
        config = component.get('config', {})
        config_props = config.get('properties', {}) if isinstance(config, dict) else {}
        all_props = {**props, **config_props}
        
        script_info = {
            'processor_id': proc.get('id'),
            'processor_name': component.get('name'),
            'command': all_props.get('Command Path', '') or all_props.get('Command', ''),
            'command_arguments': all_props.get('Command Arguments', ''),
            'working_directory': all_props.get('Working Directory', ''),
            'environment_variables': [],
            'script_body': all_props.get('Script Body', ''),
            'ignore_stdin': all_props.get('Ignore STDIN', 'false'),
            'properties': {}
        }
        
        # Extract environment variables from properties
        for prop_name, prop_value in all_props.items():
            if prop_name.startswith('env.') or 'ENVIRONMENT' in prop_name.upper():
                script_info['environment_variables'].append(f"{prop_name}={prop_value}")
            
            # Store all properties for reference
            if prop_value and len(str(prop_value)) < 500:
                script_info['properties'][prop_name] = prop_value
        
        # Only return if we found a command or script body
        if script_info['command'] or script_info['script_body']:
            return script_info
        
        return None

# ==============================================================================
# SCRIPT FILE SCANNER
# ==============================================================================

class ScriptFileScanner:
    """Scans filesystem/repository for script files"""
    
    def __init__(self, search_paths: List[str]):
        self.search_paths = search_paths
        self.found_scripts = {}
        
    def scan(self) -> Dict[str, str]:
        """Scan all search paths for script files"""
        
        print("\n🔍 Scanning for script files...")
        
        for search_path in self.search_paths:
            if not os.path.exists(search_path):
                print(f"   ⚠️  Path not found: {search_path}")
                continue
            
            print(f"   Scanning: {search_path}")
            
            if os.path.isfile(search_path):
                self._process_file(search_path)
            else:
                self._scan_directory(search_path)
        
        print(f"   ✓ Found {len(self.found_scripts)} script files")
        
        return self.found_scripts
    
    def _scan_directory(self, directory: str):
        """Recursively scan directory for scripts"""
        
        for root, dirs, files in os.walk(directory):
            # Skip hidden directories and version control
            dirs[:] = [d for d in dirs if not d.startswith('.') and d not in ['__pycache__', 'node_modules']]
            
            for filename in files:
                file_path = os.path.join(root, filename)
                self._process_file(file_path)
    
    def _process_file(self, file_path: str):
        """Process a single file if it's a script"""
        
        ext = os.path.splitext(file_path)[1].lower()
        
        if ext in SCRIPT_EXTENSIONS:
            try:
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read()
                
                self.found_scripts[file_path] = content
                
            except Exception as e:
                print(f"   ⚠️  Error reading {file_path}: {e}")

# ==============================================================================
# SCRIPT MATCHER
# ==============================================================================

class ScriptMatcher:
    """Matches NiFi processor script references to actual script files"""
    
    def __init__(self, inventory: Dict, found_scripts: Dict[str, str]):
        self.inventory = inventory
        self.found_scripts = found_scripts
        
    def match_scripts(self) -> Dict:
        """Match inventory references to actual files"""
        
        print("\n🔗 Matching script references to files...")
        
        matches = {
            'matched': [],
            'unmatched': [],
            'ambiguous': []
        }
        
        for script_ref in self.inventory['script_references']:
            command = script_ref.get('command', '')
            
            if not command:
                # Check if it's an inline script
                if script_ref.get('script_body'):
                    matches['matched'].append({
                        'processor': script_ref['processor_name'],
                        'type': 'inline',
                        'script_body': script_ref['script_body']
                    })
                continue
            
            # Extract just the script path from command
            script_path = self._extract_script_path(command)
            
            # Try to find matching file
            found_files = self._find_matching_files(script_path)
            
            if len(found_files) == 1:
                matches['matched'].append({
                    'processor': script_ref['processor_name'],
                    'processor_id': script_ref['processor_id'],
                    'reference': command,
                    'file_path': found_files[0],
                    'arguments': script_ref.get('command_arguments', '')
                })
            elif len(found_files) > 1:
                matches['ambiguous'].append({
                    'processor': script_ref['processor_name'],
                    'reference': command,
                    'possible_files': found_files
                })
            else:
                matches['unmatched'].append({
                    'processor': script_ref['processor_name'],
                    'reference': command
                })
        
        print(f"   ✓ Matched: {len(matches['matched'])}")
        print(f"   ⚠️  Unmatched: {len(matches['unmatched'])}")
        print(f"   ⚠️  Ambiguous: {len(matches['ambiguous'])}")
        
        return matches
    
    def _extract_script_path(self, command: str) -> str:
        """Extract script path from command string"""
        
        # Remove common prefixes
        command = command.strip()
        
        # Handle common patterns
        if command.startswith('/bin/bash') or command.startswith('/bin/sh'):
            parts = command.split()
            if len(parts) > 1:
                return parts[1]
        
        # Take first token that looks like a path
        for token in command.split():
            if '/' in token or token.endswith(tuple(SCRIPT_EXTENSIONS)):
                return token
        
        return command.split()[0] if command else ''
    
    def _find_matching_files(self, script_path: str) -> List[str]:
        """Find files that match the script path"""
        
        matches = []
        script_name = os.path.basename(script_path)
        
        for file_path in self.found_scripts.keys():
            # Exact match
            if file_path.endswith(script_path):
                matches.append(file_path)
            # Filename match
            elif os.path.basename(file_path) == script_name:
                matches.append(file_path)
        
        return matches

# === PAUSED HERE - CHUNK 2 ===
# === PAUSED HERE - CHUNK 2 ===

# ==============================================================================
# SCRIPT ANALYSIS ENGINE
# ==============================================================================

class ScriptAnalysisEngine:
    """Orchestrates the complete script analysis process"""
    
    def __init__(self, matched_scripts: Dict, found_scripts: Dict[str, str]):
        self.matched_scripts = matched_scripts
        self.found_scripts = found_scripts
        
    def analyze_all(self) -> Dict:
        """Analyze all matched scripts"""
        
        print("\n📊 Analyzing script contents...")
        
        analysis_results = {
            'scripts_analyzed': 0,
            'inline_scripts': [],
            'external_scripts': [],
            'operations_summary': {
                'impala': 0,
                'kudu': 0,
                'hdfs': 0,
                'hive': 0
            },
            'tables_referenced': defaultdict(list),
            'connections_found': [],
            'complexity_distribution': {
                'HIGH': 0,
                'MEDIUM': 0,
                'LOW': 0
            }
        }
        
        # Analyze inline scripts
        for match in self.matched_scripts.get('matched', []):
            if match.get('type') == 'inline':
                script_body = match.get('script_body', '')
                
                processor = ScriptProcessor(
                    f"inline_{match['processor']}", 
                    script_body
                )
                
                result = processor.analyze()
                result['processor_name'] = match['processor']
                result['script_type'] = 'inline'
                
                analysis_results['inline_scripts'].append(result)
                analysis_results['scripts_analyzed'] += 1
                
                self._update_summary(result, analysis_results)
        
        # Analyze external script files
        for match in self.matched_scripts.get('matched', []):
            if match.get('file_path'):
                file_path = match['file_path']
                content = self.found_scripts.get(file_path, '')
                
                if content:
                    processor = ScriptProcessor(file_path, content)
                    result = processor.analyze()
                    result['processor_name'] = match['processor']
                    result['processor_id'] = match['processor_id']
                    result['arguments'] = match.get('arguments', '')
                    result['script_type'] = 'external'
                    
                    analysis_results['external_scripts'].append(result)
                    analysis_results['scripts_analyzed'] += 1
                    
                    self._update_summary(result, analysis_results)
        
        # Convert defaultdict for JSON serialization
        analysis_results['tables_referenced'] = dict(analysis_results['tables_referenced'])
        
        print(f"   ✓ Analyzed {analysis_results['scripts_analyzed']} scripts")
        print(f"   Found operations: Impala={analysis_results['operations_summary']['impala']}, "
              f"Kudu={analysis_results['operations_summary']['kudu']}, "
              f"HDFS={analysis_results['operations_summary']['hdfs']}, "
              f"Hive={analysis_results['operations_summary']['hive']}")
        
        return analysis_results
    
    def _update_summary(self, result: Dict, summary: Dict):
        """Update summary statistics with script analysis results"""
        
        # Count operations
        for op_type, operations in result['operations'].items():
            summary['operations_summary'][op_type] += len(operations)
        
        # Track tables
        for table_ref in result['tables']:
            table_name = table_ref['table']
            summary['tables_referenced'][table_name].append({
                'script': result['script_name'],
                'line': table_ref['line']
            })
        
        # Track connections
        for conn in result['connections']:
            summary['connections_found'].append({
                'script': result['script_name'],
                'connection': conn['connection_string']
            })
        
        # Track complexity
        complexity = result.get('complexity', 'LOW')
        summary['complexity_distribution'][complexity] += 1

# ==============================================================================
# DEPENDENCY GRAPH BUILDER
# ==============================================================================

class DependencyGraphBuilder:
    """Builds dependency graph between flows, scripts, and database objects"""
    
    def __init__(self, analysis: Dict, inventory: Dict):
        self.analysis = analysis
        self.inventory = inventory
        
    def build_graph(self) -> Dict:
        """Build complete dependency graph"""
        
        print("\n🕸️  Building dependency graph...")
        
        graph = {
            'nodes': [],
            'edges': [],
            'layers': {
                'nifi_processors': [],
                'scripts': [],
                'database_objects': []
            }
        }
        
        # Add NiFi processor nodes
        for script_ref in self.inventory['script_references']:
            node = {
                'id': script_ref['processor_id'],
                'type': 'nifi_processor',
                'name': script_ref['processor_name'],
                'properties': script_ref.get('properties', {})
            }
            graph['nodes'].append(node)
            graph['layers']['nifi_processors'].append(node['id'])
        
        # Add script nodes and edges
        all_scripts = self.analysis['inline_scripts'] + self.analysis['external_scripts']
        
        for script in all_scripts:
            script_id = f"script_{script['script_name']}"
            
            node = {
                'id': script_id,
                'type': 'script',
                'name': script['script_name'],
                'file_type': script['file_type'],
                'complexity': script['complexity']
            }
            graph['nodes'].append(node)
            graph['layers']['scripts'].append(script_id)
            
            # Edge from processor to script
            processor_name = script.get('processor_name', '')
            processor_id = script.get('processor_id', '')
            
            if processor_id:
                graph['edges'].append({
                    'from': processor_id,
                    'to': script_id,
                    'type': 'executes'
                })
        
        # Add database object nodes and edges
        for table_name, references in self.analysis['tables_referenced'].items():
            table_id = f"table_{table_name}"
            
            node = {
                'id': table_id,
                'type': 'database_table',
                'name': table_name,
                'reference_count': len(references)
            }
            graph['nodes'].append(node)
            graph['layers']['database_objects'].append(table_id)
            
            # Edges from scripts to tables
            for ref in references:
                script_id = f"script_{ref['script']}"
                graph['edges'].append({
                    'from': script_id,
                    'to': table_id,
                    'type': 'references',
                    'line': ref.get('line')
                })
        
        print(f"   ✓ Graph built: {len(graph['nodes'])} nodes, {len(graph['edges'])} edges")
        
        return graph

# ==============================================================================
# TRANSLATION REQUIREMENTS GENERATOR
# ==============================================================================

class TranslationRequirementsGenerator:
    """Generates requirements for Databricks translation"""
    
    def __init__(self, analysis: Dict, graph: Dict):
        self.analysis = analysis
        self.graph = graph
        
    def generate_requirements(self) -> Dict:
        """Generate Databricks translation requirements"""
        
        print("\n📝 Generating translation requirements...")
        
        requirements = {
            'databricks_components_needed': [],
            'script_translations': [],
            'storage_migrations': [],
            'query_translations': []
        }
        
        # Identify Databricks components needed
        if self.analysis['operations_summary']['impala'] > 0:
            requirements['databricks_components_needed'].append({
                'component': 'Databricks SQL',
                'reason': f"{self.analysis['operations_summary']['impala']} Impala queries need translation",
                'priority': 'HIGH'
            })
        
        if self.analysis['operations_summary']['kudu'] > 0:
            requirements['databricks_components_needed'].append({
                'component': 'Delta Lake',
                'reason': f"{self.analysis['operations_summary']['kudu']} Kudu operations need Delta equivalents",
                'priority': 'HIGH'
            })
        
        if self.analysis['operations_summary']['hdfs'] > 0:
            requirements['databricks_components_needed'].append({
                'component': 'DBFS / Unity Catalog',
                'reason': f"{self.analysis['operations_summary']['hdfs']} HDFS operations need cloud storage",
                'priority': 'HIGH'
            })
        
        # Script translation requirements
        all_scripts = self.analysis['inline_scripts'] + self.analysis['external_scripts']
        
        for script in all_scripts:
            translation = {
                'original_script': script['script_name'],
                'target': 'Databricks Notebook',
                'operations': script['operations'],
                'complexity': script['complexity'],
                'estimated_effort': self._estimate_effort(script)
            }
            requirements['script_translations'].append(translation)
        
        # Storage migration requirements
        for table_name in self.analysis['tables_referenced'].keys():
            requirements['storage_migrations'].append({
                'source_table': table_name,
                'target': 'Delta Table',
                'references': len(self.analysis['tables_referenced'][table_name])
            })
        
        print(f"   ✓ Requirements generated")
        
        return requirements
    
    def _estimate_effort(self, script: Dict) -> str:
        """Estimate translation effort"""
        
        complexity = script.get('complexity', 'LOW')
        line_count = script.get('line_count', 0)
        
        total_ops = sum(len(ops) for ops in script['operations'].values())
        
        if complexity == 'HIGH' or line_count > 200 or total_ops > 10:
            return 'HIGH (3-5 days)'
        elif complexity == 'MEDIUM' or line_count > 100 or total_ops > 5:
            return 'MEDIUM (1-3 days)'
        else:
            return 'LOW (< 1 day)'

# === PAUSED HERE - CHUNK 3 ===
# === PAUSED HERE - CHUNK 3 ===

# ==============================================================================
# REPORT GENERATOR
# ==============================================================================

class ReportGenerator:
    """Generates comprehensive analysis reports"""
    
    def __init__(self, output_dir: str):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
    def generate_all_reports(self, inventory: Dict, matches: Dict, 
                            analysis: Dict, graph: Dict, requirements: Dict):
        """Generate all output files"""
        
        print("\n💾 Generating reports...")
        
        # Save inventory
        inventory_path = os.path.join(self.output_dir, 'script_inventory.json')
        with open(inventory_path, 'w') as f:
            json.dump(inventory, f, indent=2)
        print(f"   ✓ {inventory_path}")
        
        # Save matches
        matches_path = os.path.join(self.output_dir, 'script_matches.json')
        with open(matches_path, 'w') as f:
            json.dump(matches, f, indent=2)
        print(f"   ✓ {matches_path}")
        
        # Save analysis
        analysis_path = os.path.join(self.output_dir, 'script_analysis.json')
        with open(analysis_path, 'w') as f:
            json.dump(analysis, f, indent=2)
        print(f"   ✓ {analysis_path}")
        
        # Save dependency graph
        graph_path = os.path.join(self.output_dir, 'dependency_graph.json')
        with open(graph_path, 'w') as f:
            json.dump(graph, f, indent=2)
        print(f"   ✓ {graph_path}")
        
        # Save translation requirements
        requirements_path = os.path.join(self.output_dir, 'translation_requirements.json')
        with open(requirements_path, 'w') as f:
            json.dump(requirements, f, indent=2)
        print(f"   ✓ {requirements_path}")
        
        # Generate summary report
        self._generate_summary(inventory, matches, analysis, graph, requirements)
        
    def _generate_summary(self, inventory: Dict, matches: Dict, 
                         analysis: Dict, graph: Dict, requirements: Dict):
        """Generate human-readable summary report"""
        
        summary_path = os.path.join(self.output_dir, 'ANALYSIS_SUMMARY.md')
        
        with open(summary_path, 'w') as f:
            f.write("# NiFi Script Analysis Summary\n\n")
            f.write(f"**Generated:** {datetime.now().isoformat()}\n\n")
            
            f.write("## Executive Summary\n\n")
            f.write(f"- **Script Processors Found:** {inventory['total_script_processors']}\n")
            f.write(f"- **Scripts Analyzed:** {analysis['scripts_analyzed']}\n")
            f.write(f"- **Database Operations:** {sum(analysis['operations_summary'].values())}\n")
            f.write(f"- **Tables Referenced:** {len(analysis['tables_referenced'])}\n\n")
            
            f.write("## Script Inventory\n\n")
            f.write(f"- Matched Scripts: {len(matches['matched'])}\n")
            f.write(f"- Unmatched Scripts: {len(matches['unmatched'])}\n")
            f.write(f"- Ambiguous Matches: {len(matches['ambiguous'])}\n\n")
            
            f.write("## Operations Summary\n\n")
            for op_type, count in analysis['operations_summary'].items():
                f.write(f"- **{op_type.upper()}:** {count} operations\n")
            f.write("\n")
            
            f.write("## Complexity Distribution\n\n")
            for complexity, count in analysis['complexity_distribution'].items():
                f.write(f"- {complexity}: {count} scripts\n")
            f.write("\n")
            
            f.write("## Top Referenced Tables\n\n")
            sorted_tables = sorted(
                analysis['tables_referenced'].items(), 
                key=lambda x: len(x[1]), 
                reverse=True
            )[:10]
            
            for table, refs in sorted_tables:
                f.write(f"- **{table}:** {len(refs)} references\n")
            f.write("\n")
            
            f.write("## Translation Requirements\n\n")
            f.write("### Databricks Components Needed:\n\n")
            for req in requirements['databricks_components_needed']:
                f.write(f"- **{req['component']}** ({req['priority']}): {req['reason']}\n")
            f.write("\n")
            
            f.write("### Migration Effort Estimate\n\n")
            effort_counts = defaultdict(int)
            for script_trans in requirements['script_translations']:
                effort = script_trans['estimated_effort'].split()[0]
                effort_counts[effort] += 1
            
            for effort, count in sorted(effort_counts.items()):
                f.write(f"- {effort}: {count} scripts\n")
            
        print(f"   ✓ {summary_path}")

# ==============================================================================
# MAIN ORCHESTRATION
# ==============================================================================

def run_script_analysis(contract_path: str, script_paths: List[str], output_dir: str):
    """Main orchestration function"""
    
    print("="*70)
    print("NiFi Script Analysis Agent v1.0")
    print("Agent #000-1200-002")
    print("="*70)
    
    # Step 1: Build inventory from data contract
    inventory_builder = ScriptInventoryBuilder(contract_path)
    inventory = inventory_builder.build_inventory()
    
    # Step 2: Scan for script files
    scanner = ScriptFileScanner(script_paths)
    found_scripts = scanner.scan()
    
    # Step 3: Match references to files
    matcher = ScriptMatcher(inventory, found_scripts)
    matches = matcher.match_scripts()
    
    # Step 4: Analyze matched scripts
    analysis_engine = ScriptAnalysisEngine(matches, found_scripts)
    analysis = analysis_engine.analyze_all()
    
    # Step 5: Build dependency graph
    graph_builder = DependencyGraphBuilder(analysis, inventory)
    graph = graph_builder.build_graph()
    
    # Step 6: Generate translation requirements
    req_generator = TranslationRequirementsGenerator(analysis, graph)
    requirements = req_generator.generate_requirements()
    
    # Step 7: Generate reports
    report_gen = ReportGenerator(output_dir)
    report_gen.generate_all_reports(inventory, matches, analysis, graph, requirements)
    
    print("\n" + "="*70)
    print("ANALYSIS COMPLETE")
    print("="*70)
    print(f"Output directory: {output_dir}")
    print("="*70)

# ==============================================================================
# MAIN ENTRY POINT
# ==============================================================================

if __name__ == "__main__":
    """Command-line entry point"""
    
    # Default paths
    contract_path = DEFAULT_CONTRACT_PATH
    script_paths = []
    output_dir = DEFAULT_OUTPUT_DIR
    
    # Parse command line arguments
    if len(sys.argv) > 1:
        contract_path = sys.argv[1]
    
    if len(sys.argv) > 2:
        # Additional arguments are script search paths
        script_paths = sys.argv[2:]
    else:
        # Use default search paths
        print("\n⚠️  No script paths provided. Using common locations.")
        print("   To specify paths: python script_analysis_agent.py <contract.json> <path1> <path2> ...")
        print()
        
        # Try to find scripts relative to contract
        contract_dir = os.path.dirname(os.path.abspath(contract_path))
        script_paths = [
            os.path.join(contract_dir, 'scripts'),
            os.path.join(contract_dir, '..', 'scripts'),
            './scripts',
            '../scripts'
        ]
    
    # Check if contract exists
    if not os.path.exists(contract_path):
        print(f"❌ Data contract not found: {contract_path}")
        print(f"\nUsage: python script_analysis_agent.py <data_contract.json> [script_paths...]")
        sys.exit(1)
    
    # Run analysis
    try:
        run_script_analysis(contract_path, script_paths, output_dir)
    except Exception as e:
        print(f"\n❌ Error during analysis: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    sys.exit(0)