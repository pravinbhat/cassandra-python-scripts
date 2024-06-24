# This script will load the data from the input JSON file, transform it to DSBulk friendly
# JSON format and load it to Cassandra/DSE using DSBulk
#
# Author: Pravin Bhat
#
# Usage: python3 json_etl_dsbulk.py <path-to-input-json-file> <path-to-DSBulk> <path-to-scb> <client_id> <secret>
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
                    + '","metadata_s":' + json.dumps(i['metadata']) + ',"vector":[1,2]}\n')
    
def loadData(pathToDSBulk, client_id, secret, secure_connect_bundle, output_json_file_path):
    os.system(pathToDSBulk + 
              """ load -c json \
              -query 'insert into vsearch.vz_try (row_id, attributes_blob, body_blob, metadata_s, \\\"vector\\\") \
              values (:row_id, :attributes_blob, :body_blob, :metadata_s, :\\\"vector\\\")' \
              -b {secure_connect_bundle} \
              -u {client_id} \
              -p {secret} \
              -url {output_json_file_path} """
              .format(client_id=client_id, secret=secret, secure_connect_bundle=secure_connect_bundle, output_json_file_path=output_json_file_path))
    
def main():
    try:
        json_ip_file_path = sys.argv[1]
        json_op_file_path = "./output.json"
        transfromJson(json_ip_file_path, json_op_file_path)
            
        pathToDSBulk = sys.argv[2]
        secure_connect_bundle = sys.argv[3]
        client_id = sys.argv[4]
        secret = sys.argv[5]
        loadData(pathToDSBulk, client_id, secret, secure_connect_bundle, json_op_file_path)
        print(f"Successfully loaded!!")

    except Exception as e:
        print(f"An error occurred: {e}", file=sys.stderr)
        print(f"Usage: python3 json_etl_dsbulk.py <path-to-input-json-file> <path-to-DSBulk> <path-to-scb> <client_id> <secret>", file=sys.stderr)

if __name__ == "__main__":
    main()
