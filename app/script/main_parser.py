#import all relevant packages

import pandas as pd 
import csv
import xml.etree.cElementTree as et
import glob
import re 
import configparser
import datetime
import os 
import shutil

## take path & resource name from config file

config =  configparser.ConfigParser() 
config.read_file(open(r'config.txt'))
esp_resource_name = config.get("my-config", 'esp_resource_name') 
input_folder_path = config.get('my-config', 'input_folder_path') 
output_folder_path = config.get('my-config', 'output_folder_path')
log_folder_path = config.get('my config', 'log folder_path') 
manta_ext_folder_path = config.get('my-config', 'manta_ext_folder_path') 
archive_folder_path = config.get("my-config, archive_folder_path")

##globals for now, but we need to get these as inputs

node_prefix = 'N00'
node_idx = 0

edge_prefix = 'E00' 
edge_idx = 0

#dict containing child nodes of interest and their corresponding attribute [ONLY ONE For the node file)

relevant_hierarchy_child_nodes_and_attributes = {'DataMart' : 'name',
                                                 'PrimaryTable' :'name',
                                                 'JoinedTable' : 'name',
                                                 'Feed':'name'}

#dict attributes for the attribute file fore each node of Interest [NOTE: second item is a list]

relevant_attributes_for_node = {'DataMart' : ['alias', 'dbView'],
                                'PrimaryTable' : ['alias', 'name'],
                                'Feed' : ['dbTable', 'name'],
                                'MartElement' : ['is_derived_']}

#column names for each output file

node_file_columns = ['node_id','parent_node_id', 'node_name', 'node_type', 'resource_id']
edge_file_columns = ['edge_id', 'source_node_id', 'target node_id', 'edge_type', 'resource_id'] 
node_attribute_file_columns = ['node_id', 'attribute_name', 'attribute_value']
element_tag = 'element_'
table_tag = 'table_'
derived_tag = 'is_derived_'
derived_column_name_tag= 'col_name_derived_'
separator = '/'
empty = ''
space = ' '
underscore = '_'
column_type = 'Column'
transformation_type = 'Transformation' 
direct_edge_type = 'DIRECT'
indirect_edge_type = 'FILTER'

#the below variables are used for preparing and handling items extracted from the XML

node_dict_tags = {}
element_dict_tags = {}
node_list = []
node_attribute_list = []
edge_list = []
indirect_edges = [] 

#################################

def parse_all_hierarchy_child_nodes(node):
    global node_idx

    # check if this is a node of interest
    if node.tag in relevant_hierarchy_child_nodes_and_attributes.keys(): 
        attrname = relevant_hierarchy_child_nodes_and_attributes[node.tag]
        if (node.get(attrname) + underscore + node.tag) not in node_dict_tags.keys():
            node_idx += 1
            node_dict_tags[node.get(attrname) + underscore + node.tag] = node_prefix + str(node_idx) 
            #nodes picked from hierarchy file do not have any parent so we add NULL here 
            node_list.append([node_prefix + str(node_idx), empty, node.get(attrname), node.tag, esp_resource_name])
            # check if we need to pull more attributess for this tag for the attributes file 
            if node.tag in relevant_attributes_for_node.keys():
                #there could be more thatn one attrbutes of interest here, lets loop and add each one 
                # note we do not increment the node id here as its supposed to be for the same node that we currently are on
                for attrname in relevant_attributes_for_node[node.tag]:
                    if node.get(attrname) != empty: 
                        node_attribute_list.append([node_prefix + str(node_idx), attrname, node.get(attrname)])

    for childnode in list (node):
        parse_all_hierarchy_child_nodes(childnode)
        
##################################

def parse_element_origin_nodes(node): 
    global edge_file_columns
    global node_idx
    global node_dict_tags
    global element_dict_tags
    edge_set = set()
    #this is a simple file as not much nesting expected.
    for childnode in list(node):
        # now we ahve the MartElement Tag lets traverse through the desired attributes.
        #Each MartElement corresponds to one data element
        #element is the base tag that tells us the actual number of pieces in the data lineage for this data element 
        ele_idx = 1
        while childnode.get(element_tag + str(ele_idx)) not in [None, empty]:
            parent_name = childnode.get(table_tag + str(ele_idx))
            parent_id = None
            # check the is derived column and populate the variables accordingly.
            if (childnode.get(derived_tag + str(ele_idx))) == 'Y': 
                node_name = childnode.get(derived_column_name_tag + str(ele_idx)) 
                transformation = childnode.get(element_tag + str(ele_idx))
            else:
                node_name = childnode.get(element_tag + str(ele_idx))
                transformation = empty
            node_names = node_name.split(separator) 
            parent_names = parent_name.split(separator)
            for idx in range(0, len(node_names)):
                node = node_names[idx].strip()
                parent = parent_names[idx].strip()
                for parent_type in relevant_hierarchy_child_nodes_and_attributes.keys():
                    parent_key = parent + underscore + parent_type
                    if parent_key in node_dict_tags.keys():
                        parent_id = node_dict_tags[parent_key]
                        break
                if (node + underscore + parent) not in element_dict_tags.keys():
                    node_idx += 1
                    element_dict_tags[(node + underscore + parent)] = node_prefix + str(node_idx)
                    
                
                
                
                
                
                
                
                
                
                
 