
import pandas as pd
import json
import os
import re
import csv

def input_csv(full_path):
    with open(full_path, 'r') as f:
        reader = csv.reader(f)
        table_atts = list(reader)
        return table_atts[0] #for some reason its a list of list
    
def make_dict_dfs(tables):
    table_dict = {}
    for table in tables:
        df = pd.DataFrame()
        table_dict[table[:-4]] = df #remove .csv
    return table_dict

def get_tables(path):
    tables = pd.read_csv(path)
    return tables

def list_files(dir_name):
    file_names = os.listdir(dir_name)
    return file_names

def get_file(f_path): 
    with open(f_path) as json_data:
        d = json.load(json_data)
    return d

def get_att_list(table_atts,child):
    att_list = []
    for att in table_atts:
        try:
            att_list.append(child[att])
        except:
            att_list.append("None")
    return att_list

def flatten_json(table_name,table_atts,child_table_json,p_id,q_id,c_id):
    dfsent = pd.DataFrame()
    for child in child_table_json:
        att_list = get_att_list(table_atts,child)
        dftemp = pd.DataFrame([att_list],columns=table_atts)
        dftemp["policy_id"] = p_id
        dftemp["quote_id"] = q_id
        dftemp["client_id"] = c_id
        dfsent = dfsent.append(dftemp)
    return dfsent

def recurse_path(policy_data,parts):
    if(len(parts) > 1):
        child = policy_data[parts[0]]
        parts = parts.pop(0)
        recurse_path(child,parts)
    else:
        return policy_data[parts[0]]

def get_json(policy_data,table):
    table_path = table_paths[table]
    parts = table_path.split(".")
    child = recurse_path(policy_data,parts)#gets you the json array of the child element
    return child

def outputdfs(table_dict):
    for table in table_dict.keys():
        dftemp = table_dict[table]
        print(type(dftemp))
        dftemp.to_csv("/home/ec2-user/output/"+table+".csv",index=False)

def make_output_dir():
    if(not os.path.exists("/home/ec2-user/output/")):
        os.mkdir("/home/ec2-user/output/")



if __name__ == '__main__':
    table_paths = {"vehicles":"vehicles","permissiveUserCoverages":"vehicles.permissiveUserCoverages"} #dict field name: field path
    rel_path = "/home/ec2-user/data/created"
    path_to_tables = "/home/ec2-user/tables/"
    json_files = list_files(rel_path)
    tables = list_files(path_to_tables)#get all tables and their attributes
    table_dict = make_dict_dfs(tables)
    make_output_dir()
    for j in json_files:  #iterate through each file
        path = rel_path + "/"+j
        json_data = get_file(path)
        policy_data = json_data['event']['policy']
        p_id = policy_data['id'] #get the id
        q_id = json_data['event']['quoteId']
        c_id = policy_data['applicant']['clientId']
        for table_name in table_dict.keys():#get every table name and 
            child_table_json = get_json(policy_data,table_name)
            if(table_name+".csv" in tables):#make sure table object has a corresponding file
                full_path = path_to_tables+"/"+table_name+".csv"
                table_atts = input_csv(full_path)
                table_dict[table_name] = table_dict[table_name].append(flatten_json(table_name,table_atts,child_table_json,p_id,q_id,c_id))
    outputdfs(table_dict)
                

