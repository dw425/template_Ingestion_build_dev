#!/usr/bin/env python3
"""
Fixed NiFi Template Parser
Correctly parses NiFi template XML format with nested process groups
"""

import xml.etree.ElementTree as ET
import json
from pathlib import Path
from datetime import datetime
from collections import defaultdict

class NiFiTemplateParser:
    """Parser for NiFi template XML format"""
    
    def __init__(self):
        self.processors = []
        self.connections = []
        self.controller_services = []
        self.process_groups = []
        self.input_ports = []
        self.output_ports = []
        self.funnels = []
        self.labels = []
        
    def parse_template(self, xml_file):
        """Parse a NiFi template XML file"""
        
        tree = ET.parse(xml_file)
        root = tree.getroot()
        
        # Verify it's a template
        if root.tag != 'template':
            raise ValueError(f"Not a NiFi template file. Root element is: {root.tag}")
        
        # Get template metadata
        template_name = self._get_text(root, 'name', 'Unknown')
        template_desc = self._get_text(root, 'description', '')
        
        # Find the snippet element
        snippet = root.find('snippet')
        if snippet is None:
            raise ValueError("No snippet element found in template")
        
        # Parse controller services (at snippet level)
        for service in snippet.findall('controllerServices'):
            self.controller_services.append(self._parse_controller_service(service))
        
        # Parse all process groups (including nested ones)
        for group in snippet.findall('processGroups'):
            self._parse_process_group(group, parent_id=None, depth=0)
        
        return {
            'template_name': template_name,
            'template_description': template_desc,
            'statistics': self._get_statistics()
        }
    
    def _parse_process_group(self, group, parent_id=None, depth=0):
        """Recursively parse a process group and its contents"""
        
        group_id = self._get_text(group, 'id')
        group_name = self._get_text(group, 'name', 'Unknown')
        
        group_info = {
            'id': group_id,
            'name': group_name,
            'parent_id': parent_id,
            'depth': depth,
            'comments': self._get_text(group, 'comments', ''),
            'variables': self._parse_variables(group.find('variables'))
        }
        
        self.process_groups.append(group_info)
        
        # Parse the contents of this group
        contents = group.find('contents')
        if contents is not None:
            # Parse processors
            for proc in contents.findall('processors'):
                self.processors.append(self._parse_processor(proc, group_id))
            
            # Parse connections
            for conn in contents.findall('connections'):
                self.connections.append(self._parse_connection(conn, group_id))
            
            # Parse input ports
            for port in contents.findall('inputPorts'):
                self.input_ports.append(self._parse_port(port, group_id, 'input'))
            
            # Parse output ports
            for port in contents.findall('outputPorts'):
                self.output_ports.append(self._parse_port(port, group_id, 'output'))
            
            # Parse funnels
            for funnel in contents.findall('funnels'):
                self.funnels.append(self._parse_funnel(funnel, group_id))
            
            # Parse labels
            for label in contents.findall('labels'):
                self.labels.append(self._parse_label(label, group_id))
            
            # Recursively parse nested process groups
            for nested_group in contents.findall('processGroups'):
                self._parse_process_group(nested_group, parent_id=group_id, depth=depth+1)
    
    def _parse_processor(self, proc, group_id):
        """Parse a processor element"""
        
        proc_id = self._get_text(proc, 'id')
        proc_name = self._get_text(proc, 'name')
        proc_type = self._get_text(proc, 'type')
        
        # Parse configuration
        config = proc.find('config')
        config_data = {}
        if config is not None:
            config_data = {
                'scheduling_period': self._get_text(config, 'schedulingPeriod'),
                'scheduling_strategy': self._get_text(config, 'schedulingStrategy'),
                'concurrency': self._get_text(config, 'concurrentlySchedulableTaskCount'),
                'properties': self._parse_properties(config.find('properties')),
                'auto_terminated_relationships': self._parse_auto_terminated(config)
            }
        
        return {
            'id': proc_id,
            'name': proc_name,
            'type': proc_type,
            'group_id': group_id,
            'state': self._get_text(proc, 'state', 'STOPPED'),
            'config': config_data
        }
    
    def _parse_connection(self, conn, group_id):
        """Parse a connection element"""
        
        return {
            'id': self._get_text(conn, 'id'),
            'name': self._get_text(conn, 'name', ''),
            'group_id': group_id,
            'source_id': self._get_text(conn.find('source'), 'id') if conn.find('source') is not None else None,
            'source_type': self._get_text(conn.find('source'), 'type') if conn.find('source') is not None else None,
            'destination_id': self._get_text(conn.find('destination'), 'id') if conn.find('destination') is not None else None,
            'destination_type': self._get_text(conn.find('destination'), 'type') if conn.find('destination') is not None else None,
            'relationships': [r.text for r in conn.findall('.//relationship') if r.text],
            'back_pressure_data_size': self._get_text(conn, 'backPressureDataSizeThreshold'),
            'back_pressure_object_threshold': self._get_text(conn, 'backPressureObjectThreshold')
        }
    
    def _parse_controller_service(self, service):
        """Parse a controller service element"""
        
        return {
            'id': self._get_text(service, 'id'),
            'name': self._get_text(service, 'name'),
            'type': self._get_text(service, 'type'),
            'properties': self._parse_properties(service.find('properties'))
        }
    
    def _parse_port(self, port, group_id, port_type):
        """Parse an input or output port"""
        
        return {
            'id': self._get_text(port, 'id'),
            'name': self._get_text(port, 'name'),
            'type': port_type,
            'group_id': group_id,
            'state': self._get_text(port, 'state', 'STOPPED')
        }
    
    def _parse_funnel(self, funnel, group_id):
        """Parse a funnel element"""
        
        return {
            'id': self._get_text(funnel, 'id'),
            'group_id': group_id
        }
    
    def _parse_label(self, label, group_id):
        """Parse a label element"""
        
        return {
            'id': self._get_text(label, 'id'),
            'label': self._get_text(label, 'label', ''),
            'group_id': group_id,
            'width': self._get_text(label, 'width'),
            'height': self._get_text(label, 'height')
        }
    
    def _parse_properties(self, props_elem):
        """Parse properties from an element"""
        
        properties = {}
        if props_elem is not None:
            for entry in props_elem.findall('entry'):
                key_elem = entry.find('key')
                value_elem = entry.find('value')
                if key_elem is not None:
                    key = key_elem.text
                    value = value_elem.text if value_elem is not None else None
                    properties[key] = value
        return properties
    
    def _parse_variables(self, vars_elem):
        """Parse variables from a process group"""
        
        variables = {}
        if vars_elem is not None:
            for entry in vars_elem.findall('entry'):
                key_elem = entry.find('key')
                value_elem = entry.find('value')
                if key_elem is not None and key_elem.text:
                    variables[key_elem.text] = value_elem.text if value_elem is not None else None
        return variables
    
    def _parse_auto_terminated(self, config):
        """Parse auto-terminated relationships"""
        
        auto_terminated = []
        if config is not None:
            for rel in config.findall('.//autoTerminatedRelationships'):
                if rel.text:
                    auto_terminated.append(rel.text)
        return auto_terminated
    
    def _get_text(self, elem, tag, default=''):
        """Safely get text from an element"""
        
        if elem is None:
            return default
        child = elem.find(tag)
        return child.text if child is not None and child.text else default
    
    def _get_statistics(self):
        """Generate statistics about the parsed flow"""
        
        processor_types = defaultdict(int)
        for proc in self.processors:
            # Extract short type name
            proc_type = proc['type'].split('.')[-1]
            processor_types[proc_type] += 1
        
        return {
            'total_processors': len(self.processors),
            'total_connections': len(self.connections),
            'total_process_groups': len(self.process_groups),
            'total_controller_services': len(self.controller_services),
            'total_input_ports': len(self.input_ports),
            'total_output_ports': len(self.output_ports),
            'total_funnels': len(self.funnels),
            'total_labels': len(self.labels),
            'processor_types': dict(processor_types),
            'max_depth': max([g['depth'] for g in self.process_groups]) if self.process_groups else 0
        }
    
    def to_dict(self):
        """Convert parsed data to dictionary"""
        
        return {
            'processors': self.processors,
            'connections': self.connections,
            'controller_services': self.controller_services,
            'process_groups': self.process_groups,
            'input_ports': self.input_ports,
            'output_ports': self.output_ports,
            'funnels': self.funnels,
            'labels': self.labels,
            'statistics': self._get_statistics()
        }


def main():
    """Test the parser"""
    
    xml_file = "/Users/darkstar33/Desktop/NXP_Testing/ICN8_NiFi_flows_2025-05-06.xml"
    
    print(f"🔍 Parsing NiFi Template: {xml_file}")
    print("=" * 70)
    
    parser = NiFiTemplateParser()
    result = parser.parse_template(xml_file)
    
    print(f"\n✅ Template: {result['template_name']}")
    print(f"   Description: {result['template_description'][:100]}..." if result['template_description'] else "   (No description)")
    
    stats = result['statistics']
    print(f"\n📊 STATISTICS:")
    print(f"   Processors: {stats['total_processors']}")
    print(f"   Connections: {stats['total_connections']}")
    print(f"   Process Groups: {stats['total_process_groups']}")
    print(f"   Controller Services: {stats['total_controller_services']}")
    print(f"   Input Ports: {stats['total_input_ports']}")
    print(f"   Output Ports: {stats['total_output_ports']}")
    print(f"   Funnels: {stats['total_funnels']}")
    print(f"   Labels: {stats['total_labels']}")
    print(f"   Max Nesting Depth: {stats['max_depth']}")
    
    print(f"\n🔧 TOP PROCESSOR TYPES:")
    for proc_type, count in sorted(stats['processor_types'].items(), key=lambda x: x[1], reverse=True)[:10]:
        print(f"   {proc_type}: {count}")
    
    # Save to JSON
    output_file = Path(xml_file).parent / "parsed_flow_data.json"
    with open(output_file, 'w') as f:
        json.dump(parser.to_dict(), f, indent=2)
    
    print(f"\n💾 Saved full data to: {output_file}")
    print(f"\n✅ Parsing complete!")


if __name__ == "__main__":
    main()