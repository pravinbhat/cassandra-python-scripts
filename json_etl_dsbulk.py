# This script will load the data from the input JSON file, transform it to DSBulk friendly
# JSON format and load it to Astra using DSBulk
#
# Author: Pravin Bhat
#
# Usage: python3 json_etl_dsbulk.py <path-to-input-json-file> <path-to-DSBulk> <path-to-scb> <username> <password> <keyspace> <table>
#

import json
import sys
import os

def transfromJson(json_ip_file_path, json_op_file_path):
    with open(json_ip_file_path, 'r') as fi:
        data = json.load(fi)

    with open(json_op_file_path, 'w') as fo:
        for i in data:
            fo.write('{"row_id":"' + i['id'] + '","attributes_blob":"' + '","body_blob":"' + i['page_content'] 
                    + '","metadata_s":' + json.dumps(i['metadata']) + ',"vector":' + json.dumps(i['vector']) + '}\n')

def loadData(pathToDSBulk, scb, username, password, ks, table, output_json_file_path):
    os.system(pathToDSBulk + 
              """ load -c json \
              -b {scb} \
              -u {username} \
              -p {password} \
              -query 'insert into {ks}.{table} (row_id, attributes_blob, body_blob, metadata_s, \\\"vector\\\") \
              values (:row_id, :attributes_blob, :body_blob, :metadata_s, :\\\"vector\\\")' \
              -url {output_json_file_path} """
              .format(scb=scb, username=username, password=password, ks=ks, table=table, 
                       output_json_file_path=output_json_file_path))

def main():
    try:
        json_ip_file_path = sys.argv[1]
        json_op_file_path = "./output.json"
        transfromJson(json_ip_file_path, json_op_file_path)

        pathToDSBulk = sys.argv[2]
        path_to_scb = sys.argv[3]
        username = sys.argv[4]
        password = sys.argv[5]
        ks = sys.argv[6]
        table = sys.argv[7]
        loadData(pathToDSBulk, path_to_scb, username, password, ks, table, json_op_file_path)
        print(f"Successfully loaded!!")

    except Exception as e:
        print(f"An error occurred: {e}", file=sys.stderr)
        print(f"Usage: python3 json_etl_dsbulk.py <path-to-input-json-file> <path-to-DSBulk> <path-to-scb>" + 
              "<username> <password> <keyspace> <table>", file=sys.stderr)

if __name__ == "__main__":
    main()
