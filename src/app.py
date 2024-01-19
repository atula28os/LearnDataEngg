import os
import sys
import json
import glob
import re
import pandas as pd


def list_files(src_location, pattern=None):
    if pattern:
        src_location = f'{src_location}/{pattern}'
        
    return glob.glob(src_location, recursive=True)

def get_column_name(schema, category, sort_key='column_position'):
    """This Function helps in getting column names sorted in order of column_position.
    Args:
        schema: json data loaded from schema.json file
        category: name of table (e.g. orders, customers etc.)
        sort_key: column_position is the default sort key used to sort the columns
    Returns: List of Column Names
    """
    try:
        if category in schema.keys():
            category_data = schema[category]
            columns = sorted(category_data, key=lambda col: col[sort_key])
            return [column['column_name'] for column in columns] 
        else:
            return None
    except Exception as ex:
        print(ex)

def get_filename_from_path(file_path, pattern='[/\\\]'):
    splitted_file_path = re.split('[/\\\]', file_path)
    file_name = splitted_file_path[-2]
    return file_name

def read_df(file_path, column_names):
    df = pd.read_csv(filepath_or_buffer=file_path, names=column_names)
    return df

def create_target_folder(tgt_base_dir, folder_name=None):
    target_base_path = tgt_base_dir
    if folder_name is None:
        folder_path = target_base_path
    else:
        folder_path = f'{target_base_path}/{folder_name}'
    return os.makedirs(name=target_base_path, exist_ok=True)

def generate_target_destination(tgt_base_dir, file_name, extension):
    target_base_path = tgt_base_dir
    file_name = f'{target_base_path}/{file_name}.{extension}'
    return file_name

def generate_json_doc(df, json_file_path):
    try:
        df.to_json(
            json_file_path,
            orient='records',
            lines=True
        )
        return True
    except Exception as ex:
        return False
    
def csv_to_json_converter(ds_name):
    
    src_base_dir = os.environ.get('SOURCE_DIR')
    tgt_bae_dir = os.environ.get('TARGET_DIR')

    schemas = json.load(open(f'{src_base_dir}/schemas.json'))
    files = glob.glob(f'{src_base_dir}/{ds_name}/part-*')

    for file in files:
        # FILE NAME
        file_name = get_filename_from_path(file)

        # COLUMNS
        column_names = get_column_name(schemas, file_name)

        # TO DF
        df = read_df(file, column_names)

        # TARGET FILE LOC
        json_file_path = generate_target_destination(tgt_bae_dir, file_name, 'json')

        # DF TO JSON
        status = generate_json_doc(df, json_file_path)

        if status:
            print(f'{file_name}.json created!')


if __name__ == '__main__':

    ds_name = sys.argv[1]
    csv_to_json_converter(ds_name)