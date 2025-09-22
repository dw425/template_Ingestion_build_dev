#!/usr/bin/env python3
"""
Extract script references from Agent #002 output
Shows what scripts were identified in the original NiFi flow
"""

import json
import sys

def extract_script_info(inventory_path):
    """Extract and display script references"""
    
    with open(inventory_path, 'r') as f:
        inventory = json.load(f)
    
    print("="*70)
    print("SCRIPT REFERENCES FROM NIFI FLOW")
    print("="*70)
    
    print(f"\nTotal ExecuteStreamCommand processors: {inventory['total_script_processors']}")
    print(f"Unique script commands: {inventory['summary']['unique_scripts']}")
    
    print("\n" + "="*70)
    print("SCRIPT COMMANDS FOUND:")
    print("="*70)
    
    # Show most common commands
    print("\nMost common commands:")
    for cmd, count in inventory['summary']['most_common_commands']:
        print(f"  {count}x: {cmd}")
    
    print("\n" + "="*70)
    print("DETAILED SCRIPT REFERENCES:")
    print("="*70)
    
    # Group by command for easier review
    by_command = {}
    for script_ref in inventory['script_references']:
        cmd = script_ref.get('command', 'NO_COMMAND')
        if cmd not in by_command:
            by_command[cmd] = []
        by_command[cmd].append(script_ref)
    
    for cmd, refs in sorted(by_command.items()):
        print(f"\nCommand: {cmd}")
        print(f"Used by {len(refs)} processor(s):")
        
        for ref in refs[:3]:  # Show first 3 processors using this command
            print(f"  - {ref['processor_name']}")
            if ref.get('command_arguments'):
                print(f"    Args: {ref['command_arguments'][:100]}")
            if ref.get('working_directory'):
                print(f"    Working dir: {ref['working_directory']}")
        
        if len(refs) > 3:
            print(f"  ... and {len(refs) - 3} more")
    
    print("\n" + "="*70)
    print("WORKING DIRECTORIES:")
    print("="*70)
    for wd in inventory.get('working_directories', []):
        print(f"  {wd}")
    
    print("\n" + "="*70)
    print("ENVIRONMENT VARIABLES:")
    print("="*70)
    env_vars = inventory.get('environment_variables', [])
    if env_vars:
        for env in env_vars:
            print(f"  {env}")
    else:
        print("  None found")
    
    print("\n" + "="*70)
    print("INLINE SCRIPTS:")
    print("="*70)
    
    inline_count = 0
    for script_ref in inventory['script_references']:
        if script_ref.get('script_body'):
            inline_count += 1
            print(f"\nProcessor: {script_ref['processor_name']}")
            script_body = script_ref['script_body']
            print(f"Script (first 200 chars):")
            print(f"  {script_body[:200]}...")
    
    if inline_count == 0:
        print("  No inline scripts found (all are external file references)")
    else:
        print(f"\nTotal inline scripts: {inline_count}")


if __name__ == "__main__":
    inventory_path = "script_analysis/script_inventory.json"
    
    if len(sys.argv) > 1:
        inventory_path = sys.argv[1]
    
    try:
        extract_script_info(inventory_path)
    except FileNotFoundError:
        print(f"Error: Could not find {inventory_path}")
        print("Run Agent #002 first to generate the inventory file")
        sys.exit(1)