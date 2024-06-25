# This script will load the data from the input JSON file, transform it to DSBulk friendly
# JSON format and load it to Cassandra/DSE using DSBulk
#
# Author: Pravin Bhat
#
# Usage: python3 json_etl_dsbulk.py <path-to-input-json-file> <path-to-DSBulk> <host> <port>
#           <username> <password> <keyspace> <table> <truststore_path> <truststore_password>
#

import json
import sys
import os

class markup:
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def transfromJson(json_ip_file_path, json_op_file_path):
    with open(json_ip_file_path, 'r') as fi:
        data = json.load(fi)

    with open(json_op_file_path, 'w') as fo:
        for i in data:
            fo.write('{"row_id":"' + i['id'] + '","attributes_blob":"' + '","body_blob":"' + i['page_content'] 
                + '","metadata_s":' + json.dumps(i['metadata']) + ',"vector":' + json.dumps(i['vector']) + '}\n')

def loadData(pathToDSBulk, host, port, username, password, ks, table, truststore_path, truststore_password, output_json_file_path, cl):
    os.system(pathToDSBulk + 
              """ load -c json \
              -h {host} -port {port} \
              -u {username} \
              -p {password} \
              -query 'insert into {ks}.{table} (row_id, attributes_blob, body_blob, metadata_s, \\\"vector\\\") \
              values (:row_id, :attributes_blob, :body_blob, :metadata_s, :\\\"vector\\\")' \
              --datastax-java-driver.advanced.ssl-engine-factory.class DefaultSslEngineFactory \
              --datastax-java-driver.advanced.ssl-engine-factory.truststore-path {truststore_path} \
              --datastax-java-driver.advanced.ssl-engine-factory.truststore-password {truststore_password} \
              --datastax-java-driver.advanced.ssl-engine-factory.hostname-validation false \
              --driver.advanced.auth-provider.class DsePlainTextAuthProvider \
              -cl {cl} -url {output_json_file_path} """
              .format(host=host, port=port, username=username, password=password, ks=ks, table=table, 
                      truststore_path=truststore_path, truststore_password=truststore_password, 
                       output_json_file_path=output_json_file_path, cl=cl))

def main():
    try:
        json_ip_file_path = sys.argv[1]
        json_op_file_path = "./output.json"
        transfromJson(json_ip_file_path, json_op_file_path)

        pathToDSBulk = sys.argv[2]
        host = sys.argv[3]
        port = sys.argv[4]
        username = sys.argv[5]
        password = sys.argv[6]
        ks = sys.argv[7]
        table = sys.argv[8]
        truststore_path = sys.argv[9]
        truststore_password = sys.argv[10]
        if len(sys.argv) > 11:
            cl = sys.argv[11].upper()
            print(f"{markup.OKBLUE}Using provided consistency level: {cl} for loading data via DSBulk!{markup.ENDC}")
        else:
            cl = "ALL"
            print(f"{markup.WARNING}{markup.BOLD}{markup.UNDERLINE}Using consistency level: {cl} for loading data via DSBulk, as no value provided via command line.{markup.ENDC}")
        loadData(pathToDSBulk, host, port, username, password, ks, table, truststore_path, truststore_password, json_op_file_path, cl)
        print(f"{markup.OKGREEN}Data loaded successfully via DSBulk, see logs for details!!{markup.ENDC}")

    except Exception as e:
        print(f"An error occurred: {e}", file=sys.stderr)
        print(f"{markup.FAIL}Usage: python3 json_etl_dsbulk.py <path-to-input-json-file> <path-to-DSBulk> <host> <port> <username>{markup.ENDC}" + 
              f"{markup.FAIL} <password> <keyspace> <table> <truststore_path> <truststore_password> <consistency-level>{markup.ENDC}", file=sys.stderr)

if __name__ == "__main__":
    main()
