#!/usr/bin/env python3
"""
Data Contract Analyzer
Extracts key information for migration planning from the data contract JSON
"""

import json
import sys
from collections import Counter, defaultdict

def analyze_data_contract(contract_path):
    """Extract migration-relevant information from data contract"""
    
    with open(contract_path, 'r') as f:
        contract = json.load(f)
    
    print("="*70)
    print("DATA CONTRACT ANALYSIS - MIGRATION PLANNING")
    print("="*70)
    
    # 1. Processor Type Summary
    print("\n📊 PROCESSOR TYPES (Top 20):")
    processor_types = contract.get('statistics', {}).get('processor_types', {})
    for proc_type, count in sorted(processor_types.items(), key=lambda x: x[1], reverse=True)[:20]:
        print(f"   {proc_type}: {count}")
    
    # 2. Database/Storage Related Processors
    print("\n💾 DATABASE & STORAGE PROCESSORS:")
    db_keywords = ['SQL', 'Database', 'Kudu', 'Hive', 'HBase', 'Impala', 'Query']
    storage_keywords = ['HDFS', 'S3', 'File', 'Put', 'Get']
    
    db_processors = {k: v for k, v in processor_types.items() 
                    if any(kw in k for kw in db_keywords)}
    storage_processors = {k: v for k, v in processor_types.items() 
                         if any(kw in k for kw in storage_keywords)}
    
    print("   Database processors:")
    for proc, count in sorted(db_processors.items(), key=lambda x: x[1], reverse=True):
        print(f"      {proc}: {count}")
    
    print("   Storage processors:")
    for proc, count in sorted(storage_processors.items(), key=lambda x: x[1], reverse=True):
        print(f"      {proc}: {count}")
    
    # 3. Expression Inventory Summary
    print("\n🔍 EXPRESSION INVENTORY:")
    expr_inv = contract.get('expression_inventory', {})
    summary = expr_inv.get('summary', {})
    
    print(f"   Total expressions: {summary.get('total_expressions', 0)}")
    print(f"   Processors with expressions: {summary.get('total_processors_with_expressions', 0)}")
    print(f"\n   By category:")
    for cat, count in summary.get('by_category', {}).items():
        print(f"      {cat}: {count}")
    
    # 4. Environment Variables (potential DB connections)
    print("\n🔌 ENVIRONMENT VARIABLES (potential connections):")
    env_vars = expr_inv.get('environment_variables', {})
    if env_vars:
        for var_name in sorted(env_vars.keys())[:20]:
            usage_count = len(env_vars[var_name])
            print(f"   {var_name}: used {usage_count} times")
    else:
        print("   None found")
    
    # 5. Flow File Attributes (data references)
    print("\n📝 TOP FLOW FILE ATTRIBUTES:")
    ff_attrs = expr_inv.get('flow_file_attributes', {})
    if ff_attrs:
        sorted_attrs = sorted(ff_attrs.items(), key=lambda x: len(x[1]), reverse=True)
        for attr_name, usages in sorted_attrs[:15]:
            print(f"   ${{{attr_name}}}: used {len(usages)} times")
    
    # 6. Critical Components
    print("\n⚠️  CRITICAL COMPONENTS FOR MIGRATION:")
    critical = contract.get('critical_components', {})
    
    scripts = critical.get('scripts', [])
    print(f"   Scripts: {len(scripts)}")
    for script in scripts[:5]:
        print(f"      - {script.get('name')} ({script.get('type')}) - {script.get('complexity')} complexity")
    
    sql_queries = critical.get('sql_queries', [])
    print(f"   SQL Queries: {len(sql_queries)}")
    for sql in sql_queries[:5]:
        print(f"      - {sql.get('name')} ({sql.get('type')})")
    
    transforms = critical.get('transforms', [])
    print(f"   Transforms: {len(transforms)}")
    
    # 7. Sample Processors with DB Connections
    print("\n🔗 SAMPLE DATABASE PROCESSOR CONFIGURATIONS:")
    components = contract.get('components', {})
    processors = components.get('processors', [])
    
    db_proc_samples = []
    for proc in processors:
        comp = proc.get('component', {})
        proc_type = comp.get('type', '')
        
        if any(kw in proc_type for kw in ['SQL', 'Database', 'Kudu', 'Query']):
            config = comp.get('config', {})
            props = config.get('properties', {}) if isinstance(config, dict) else comp.get('properties', {})
            
            db_proc_samples.append({
                'name': comp.get('name'),
                'type': proc_type,
                'properties': {k: v for k, v in props.items() if v and len(str(v)) < 200}
            })
            
            if len(db_proc_samples) >= 3:
                break
    
    for sample in db_proc_samples:
        print(f"\n   Processor: {sample['name']}")
        print(f"   Type: {sample['type']}")
        print(f"   Properties:")
        for prop, val in sample.get('properties', {}).items():
            if 'password' not in prop.lower():  # Don't print passwords
                print(f"      {prop}: {val}")
    
    # 8. File Path Analysis
    print("\n📁 FILE PATH PATTERNS:")
    path_patterns = set()
    
    for proc in processors:
        comp = proc.get('component', {})
        config = comp.get('config', {})
        props = config.get('properties', {}) if isinstance(config, dict) else comp.get('properties', {})
        
        for prop_name, prop_value in props.items():
            if isinstance(prop_value, str) and ('/' in prop_value or 'hdfs' in prop_value.lower()):
                # Extract path pattern
                if len(prop_value) < 200:
                    path_patterns.add(prop_value)
    
    if path_patterns:
        print("   Sample paths found:")
        for path in sorted(list(path_patterns))[:10]:
            print(f"      {path}")
    
    # 9. Migration Readiness
    print("\n📈 MIGRATION METRICS:")
    stats = contract.get('statistics', {})
    print(f"   Total components: {stats.get('total_components', 0)}")
    print(f"   Complexity score: {stats.get('complexity_score', 0)}/100")
    print(f"   Migration readiness: {stats.get('migration_readiness', 0)}%")
    
    validation = contract.get('validation', {})
    print(f"   Validation warnings: {len(validation.get('warnings', []))}")
    
    security = contract.get('security', {})
    print(f"   Security findings: {len(security.get('findings', []))}")
    
    print("\n" + "="*70)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        contract_path = "/Users/darkstar33/Desktop/NXP_Testing/ICN8_NiFi_flows_2025-05-06_analysis/data_contract.json"
        print(f"Using default path: {contract_path}\n")
    else:
        contract_path = sys.argv[1]
    
    analyze_data_contract(contract_path)