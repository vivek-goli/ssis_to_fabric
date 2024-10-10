import json
import msal
import requests
from lxml import etree
import re
import base64
import pyodbc
import logging
import config

class SSIS_Fabric:
    #class variables
    client_id = config.CLIENT_ID
    tenant_id = config.TENANT_ID
    scope = [config.SCOPE]
    authority = f"{config.AUTHORITY}{tenant_id}"
    username = config.USERNAME
    password = config.PASSWORD
    namespaces = {'DTS': config.NAMESPACE1, 'SQLTask': config.NAMESPACE2}
    api_base_url = config.API_URL

    #initialize the instance variables
    def __init__(self, workspace, lakehouse, warehouse, pipeline_name, warehouse_endpoint):
        self.workspace = workspace
        self.lakehouse = lakehouse
        self.warehouse = warehouse
        self.pipeline_name = pipeline_name
        self.endpoint = warehouse_endpoint
        self.workspace_id = ""
        self.lakehouse_id = ""
        self.warehouse_id = ""
        self.access_token = ""
        self.component_map = {}
        self.flows = {}
        self.dependency_map = {}
        self.executables = {}
        self.warehouse_items = {"tables":[], "procedures":[]}
        self.counts = {"copy":1, "procedure":1, "wait":1}
        logging.info("Input details received")
    
    def create_token(self):
        try:
            app = msal.PublicClientApplication(SSIS_Fabric.client_id, authority=SSIS_Fabric.authority)
            result = app.acquire_token_by_username_password(username=SSIS_Fabric.username, password=SSIS_Fabric.password, scopes=SSIS_Fabric.scope)

            self.access_token = result["access_token"]
            logging.info("Access token acquired")
        except Exception as e:
            logging.error(f"Function create_token(), Failed to acquire token.")
            raise

    def get_workspace_id(self):
        try:
            headers = {"Authorization": f"Bearer {self.access_token}"}
            response = requests.get(f"{SSIS_Fabric.api_base_url}workspaces", headers=headers)
            response_body = response.json()
            for value in response_body["value"]:
                if value["displayName"] == self.workspace:
                    self.workspace_id = value["id"]
                    logging.info(f"Received the workspace id {self.workspace_id}")
        except Exception as e:
            logging.error(f"Function: get_workspace_id(), Failed to get workspace id.")
            raise
    
    def get_warehouse_id(self):
        url = f"{SSIS_Fabric.api_base_url}workspaces/{self.workspace_id}/warehouses"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
        try:
            response = requests.get(url, headers=headers)
            artifacts = response.json().get('value', [])
            warehouses = [artifact for artifact in artifacts if artifact['type'] == 'Warehouse']
            if warehouses:
                for warehouse in warehouses:
                    if warehouse["displayName"] == self.warehouse:
                        self.warehouse_id = warehouse["id"]
                        logging.info(f"Received the warehouse id {self.warehouse_id}")
            else:
                logging.info(f"No warehouse found in workspace '{self.workspace}'")
        except Exception as e:
            logging.error(f"Function: get_warehouse_id(), Failed to retrieve warehouse.\nStatus code: {response.status_code}\nResponse:", response.text)
            raise

    def get_lakehouse_id(self):
        url = f"{SSIS_Fabric.api_base_url}workspaces/{self.workspace_id}/lakehouses"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
        try:
            response = requests.get(url, headers=headers)
            artifacts = response.json().get('value', [])
            lakehouses = [artifact for artifact in artifacts if artifact['type'] == 'Lakehouse']
            if lakehouses:
                for lakehouse in lakehouses:
                    if lakehouse["displayName"] == self.lakehouse:
                        self.lakehouse_id = lakehouse["id"]
                        logging.info(f"Received the lakehouse id '{self.lakehouse_id}'")
            else:
                logging.info(f"No lakehouse found in workspace '{self.workspace}'")
        except Exception as e:
            logging.error(f"Function: get_lakehouse_id(), Failed to retrieve lakehouse.\nStatus code: {response.status_code}\nResponse: {response.text}")
            raise

    @staticmethod 
    def get_input_columns_for_merge(inputs): #returns the input column names for one side input of merge join
        cols = []
        sort_map = {}
        try:
            for i in range(len(inputs)):
                col_name = inputs[i].xpath("@cachedName")[0]
                cols.append(col_name)
                sortid = inputs[i].xpath("@cachedSortKeyPosition")
                if len(sortid) > 0:
                    sort_map[int(sortid[0])] = col_name
            return cols, sort_map
        except Exception as e:
            logging.error(f"Function: get_input_columns_for_merge(), Failed to get input columns for Merge Join.")
            raise

    @staticmethod
    def parse_source_component(dataflow, name): # returns the table name from the source, same name is used as destination in copy acitvity
        try:
            component = dataflow.xpath(f"DTS:ObjectData/pipeline/components/component[@name='{name}']", namespaces=SSIS_Fabric.namespaces)[0]
            columns_path = "outputs/output[contains(@name, 'Source Output')]/outputColumns/outputColumn"
            columns = component.xpath(f"{columns_path}/@name")
            datatypes = component.xpath(f"{columns_path}/@dataType")
            source = component.xpath("properties/property[@name='OpenRowset']/text()")[0]
            matches = re.findall(r'\[([^\]]+)\]', source)
            logging.info(f"{matches[1]} columns : {columns} dataypes: {datatypes}")
            return matches[1], columns, datatypes
        except Exception as e:
            logging.error(f"Function: parse_source_component(), Error occurred while parsing the source component '{name}'")
            raise
    
    @staticmethod
    def parse_destination_component(dataflow, name): # returns the final destination table name
        try:
            component = dataflow.xpath(f"DTS:ObjectData/pipeline/components/component[@name='{name}']", namespaces=SSIS_Fabric.namespaces)[0]
            destination = component.xpath("properties/property[@name='OpenRowset']/text()")[0]
            matches = re.findall(r'\[([^\]]+)\]', destination)
            columns_path = "inputs/input[contains(@name, 'Destination Input')]/externalMetadataColumns/externalMetadataColumn"
            columns = component.xpath(f"{columns_path}/@name")
            datatypes = component.xpath(f"{columns_path}/@dataType")
            return matches[-1], columns, datatypes
        except Exception as e:
            logging.error(f"Function: parse_destination_component(), Error occurred while parsing the destination component '{name}'")
            raise
    
    @staticmethod
    def parse_execsql(execsql, name): # returns the procedure name that is executed in this task
        try:
            statement = execsql.xpath("//DTS:ObjectData/SQLTask:SqlTaskData/@SQLTask:SqlStatementSource", namespaces=SSIS_Fabric.namespaces)[0]
            procedure_name = statement[5:]
            return procedure_name
        except Exception as e:
            logging.error(f"Function: parse_execsql(), Error occured while parsing the '{name}' component")
            raise

    @staticmethod
    def parse_merge(dataflow, name, table1, table2): # returns the query required to join the two tables and create new table
        try:
            merge = dataflow.xpath(f"DTS:ObjectData/pipeline/components/component[@name='{name}']", namespaces=SSIS_Fabric.namespaces)[0]
            joins = {0:"FULL OUTER JOIN", 1:"LEFT OUTER JOIN", 2:"INNER JOIN"}
            joinid = int(merge.xpath("properties/property[@name='JoinType']/text()")[0])
            key_count = int(merge.xpath("properties/property[@name='NumKeyColumns']/text()")[0])
            join = joins[joinid]

            inputs = merge.xpath("inputs/input")
            left_inputs = inputs[0].xpath("inputColumns/inputColumn")
            right_inputs = inputs[1].xpath("inputColumns/inputColumn")
            
            left_cols, left_sort = SSIS_Fabric.get_input_columns_for_merge(left_inputs)
            right_cols, right_sort = SSIS_Fabric.get_input_columns_for_merge(right_inputs)
        except Exception as e:
            logging.error(f"Function: parse_merge(), Error parsing the '{name}' component for inputs.")
            raise

        try:
            merge_output = "outputs/output[@name='Merge Join Output']/outputColumns/outputColumn"
            output_cols = merge.xpath(f"{merge_output}/@name")
            old_cols_text = merge.xpath(f"{merge_output}/properties/property[@name='InputColumnID']/text()")
            old_col_names = []
            for old_col_text in old_cols_text:
                matches = re.findall(r'\[([^\]]+)\]', old_col_text)
                old_col_names.append(matches[1])
            logging.info(f"Left Input columns for {name} are {left_cols} sorted as {left_sort}")
            logging.info(f"Right Input columns for {name} are {right_cols} sorted as {right_sort}")
            logging.info(f"Output columns from {name} are {output_cols}")
        except Exception as e:
            logging.error(f"Function: parse_merge(), Error parsing the '{name}' component for outputs.")
            raise
        
        try:
            query = "SELECT "
            for i in range(len(old_col_names)):
                if old_col_names[i] in left_cols:
                    query = query + f"t1.{old_col_names[i]} AS {output_cols[i]}, "
                elif old_col_names[i] in right_cols:
                    query = query + f"t2.{old_col_names[i]} AS {output_cols[i]}, "
            
            if inputs[0].xpath("@name")[0] == "Merge Join Right Input": # inputs swapped
                query = query[:-2] + f" FROM schema.{table2} AS t1 {join} schema.{table1} AS t2 ON t1.{left_sort[1]} = t2.{right_sort[1]}"
            else:
                query = query[:-2] + f" FROM schema.{table1} AS t1 {join} schema.{table2} AS t2 ON t1.{left_sort[1]} = t2.{right_sort[1]}"

            if key_count > 1:
                query = query + " WHERE "
                for i in range(2, key_count + 1):
                    query += f" AND t1.{left_sort[i]} = t2.{right_sort[i]}" if i > 2 else f" t1.{left_sort[i]} = t2.{right_sort[i]}"
            query += ";"   
            return query
        except Exception as e:
            logging.error(f"Function: parse_merge(), Error writing the select statement for '{name}' component.")
            raise
  
    @staticmethod
    def parse_lookup(dataflow, name, table1, columns): # returns the query required to join the two tables and create new table
        try:
            lookup = dataflow.xpath(f"DTS:ObjectData/pipeline/components/component[@name='{name}']", namespaces=SSIS_Fabric.namespaces)[0]
            lookup_details = lookup.xpath("properties/property[@name='SqlCommand']/text()")
            pattern = r"FROM\s+\[([^\]]+)\]\.\[([^\]]+)\]"
            match = re.search(pattern, lookup_details[0], re.IGNORECASE)
            lookup_schema = match.group(1)
            lookup_table = match.group(2)

            input_lookup = "inputs/input/inputColumns/inputColumn"
            join_col1 = lookup.xpath(f"{input_lookup}/@cachedName")
            join_col2 = lookup.xpath(f"{input_lookup}/properties/property[@name='JoinToReferenceColumn']/text()")
            joining_datatypes = lookup.xpath(f"{input_lookup}/@cachedDataType")
            lookup_output_path = "outputs/output[contains(@name, 'Lookup Match Output')]/outputColumns/outputColumn"
            old_cols = lookup.xpath(f"{lookup_output_path}/properties/property[@name='CopyFromReferenceColumn']/text()")
            ref_cols = lookup.xpath(f"{lookup_output_path}/@name")
            datatypes = lookup.xpath(f"{lookup_output_path}/@dataType")
            logging.info(f"Input columns for {name} are {columns}")
            logging.info(f"Referred columns in {name} are {ref_cols}")
        except Exception as e:
            logging.error(f"Function: parse_lookup(), Error in parsing '{name}' component for inputs and outputs.")
            raise
        
        try:
            query = "SELECT "
            for col in columns:
                query = query + f"t1.{col}, "
            for i in range(len(ref_cols)):
                query = query + f"t2.{old_cols[i]} AS {ref_cols[i]}, "
            query = query[:-2] + f" FROM schema.{table1} AS t1 JOIN schema.{lookup_table} AS t2 ON t1.{join_col1[0]} = t2.{join_col2[0]}"

            n1 = len(join_col1)
            n2 = len(join_col2)
            if n1 > 1 and n2 > 1:
                query += " WHERE"
                for i in range(1, n1):
                    query += f" t1.{join_col1[i]} = t2.{join_col2[i]}"
            return lookup_table, query, join_col2 + old_cols, joining_datatypes + datatypes
        except Exception as e:
            logging.error(f"Function: parse_lookup(), Error in writing the select statement for '{name}' component.")
            raise
    
    @staticmethod
    def get_columns_from_sort(dataflow, sort_name):
        try:
            component = dataflow.xpath(f"//components/component[@name='{sort_name}']")[0]
            sort_output = "outputs/output[@name='Sort Output']/outputColumns/outputColumn"
            columns = component.xpath(f"{sort_output}/@name")
            datatypes = component.xpath(f"{sort_output}/@dataType")
            return columns, datatypes        
        except Exception as e:
            logging.error(f"Function: get_columns_from_sort(), An unexpected error occurred while parsing the sort component '{sort_name}'.")
            raise

    def get_columns_for_lookup(self, dataflow, component_name):
        try:
            component = dataflow.xpath(f"DTS:ObjectData/pipeline/components/component[@name='{component_name}']", namespaces=SSIS_Fabric.namespaces)[0]
            if self.component_map[component_name][0] == "Microsoft.Lookup":
                prev_comp = self.dependency_map[component_name][0]
                lookup = dataflow.xpath(f"DTS:ObjectData/pipeline/components/component[@name='{component_name}']", namespaces=SSIS_Fabric.namespaces)[0]
                ref_cols = lookup.xpath("outputs/output[contains(@name, 'Lookup Match Output')]/outputColumns/outputColumn/@name")
                return self.get_columns_for_lookup(dataflow, prev_comp) + ref_cols
            elif self.component_map[component_name][0] == "Microsoft.OLEDBSource":
                columns =  component.xpath("outputs/output[contains(@name, 'Source Output')]/outputColumns/outputColumn/@name")
                return columns
            elif self.component_map[component_name][0] == "Microsoft.MergeJoin":
                columns =  component.xpath("outputs/output[@name='Merge Join Output']/outputColumns/outputColumn/@name")
                return columns
        except Exception as e:
            logging.error(f"Function: get_columns_for_lookup(), Error in getting the input received by lookup component.")
            raise

    def copy_activity_json(self, schema, table_name, activity_name, columns, prev_executable=None): # creates the json for the copy activity from source to stage schema in warehouse
        try:
            with open(f"{config.TEMPLATES_FOLDER}copyactivity_wh.json", "r") as file:
                copy = json.load(file)
            warehouse_details = {
                "endpoint": self.endpoint,
                "artifactId": self.warehouse_id,
                "workspaceId": self.workspace_id
            }
            copy["name"] = activity_name
            if prev_executable:
                depends = self.executables[prev_executable]
                for d in depends:
                    copy["dependsOn"] += [{"activity": d, "dependencyConditions": ["Succeeded"]}]

            copy["typeProperties"]["sink"]["datasetSettings"]["linkedService"]["name"] = self.warehouse
            copy["typeProperties"]["sink"]["datasetSettings"]["linkedService"]["properties"]["typeProperties"] = warehouse_details        
            copy["typeProperties"]["sink"]["datasetSettings"]["typeProperties"] = {
                "schema": schema,
                "table": table_name
            }
            mappings = []
            for col in columns:
                mappings += [{"source": {"name": col},"sink": {"name": col}}]

            copy["typeProperties"]["translator"]["mappings"] = mappings

            with open(f"{config.TEMPLATES_FOLDER}pipeline.json", "r+") as file:
                pipeline = json.load(file)
                pipeline["properties"]["activities"] += [copy]
                file.seek(0)        
                json.dump(pipeline, file, indent=4)
                file.truncate()
            logging.info(f"JSON for {activity_name} created and added to pipeline.json")
        except Exception as e:
            logging.error(f"Function: copy_activity_json(), Error in creating a json for {activity_name}.")
            raise

    def procedure_json(self, procedure_name, activity_name, depends): # creates a json for the storedprocedure acitivity for merge join / lookup
        try:
            with open(f"{config.TEMPLATES_FOLDER}wait.json", "r") as file:
                wait = json.load(file)
            with open(f"{config.TEMPLATES_FOLDER}procedure_activity.json", "r") as file:
                procedure = json.load(file)
            
            wait_name = f"Wait{self.counts["wait"]}"
            wait["name"] = wait_name
            for d in depends:
                wait["dependsOn"] += [{"activity": d, "dependencyConditions": ["Succeeded"]}]
            self.counts["wait"] += 1
            warehouse_details = {
                "endpoint": self.endpoint,
                "artifactId": self.warehouse_id,
                "workspaceId": self.workspace_id
            }
            procedure["name"] = activity_name
            procedure["dependsOn"] = [{"activity": wait_name, "dependencyConditions": ["Succeeded"]}]
            procedure["typeProperties"]["storedProcedureName"] = procedure_name
            procedure["linkedService"]["name"] = self.warehouse
            procedure["linkedService"]["objectId"] = self.warehouse_id
            procedure["linkedService"]["properties"]["typeProperties"] = warehouse_details

            with open(f"{config.TEMPLATES_FOLDER}pipeline.json", "r+") as file:
                pipeline = json.load(file)
                pipeline["properties"]["activities"] += [wait, procedure]
                file.seek(0)        
                json.dump(pipeline, file, indent=4)
                file.truncate()
            logging.info(f"JSON for {activity_name} created successfully.")
        except Exception as e:
            logging.error(f"Function: procedure_json(), Error in creating a json for {activity_name}.")
            raise

    def design_create_procedure(self, procedure_name, destination_table, query):
        try:
            self.warehouse_items["procedures"] = self.warehouse_items["procedures"] + [procedure_name] 
            statement = f"""
                        CREATE PROCEDURE {procedure_name} AS
                        BEGIN
                            IF OBJECT_ID('{destination_table}', 'U') IS NOT NULL
                            BEGIN
                                DROP TABLE {destination_table};
                            END
                            CREATE TABLE {destination_table} AS
                            {query}
                        END;
                    """
            return statement
        except Exception as e:
            logging.error(f"Function: design_create_procedure(), Error making the create procedure statement for {procedure_name}.")
            raise

    def design_create_table(self, table_name, columns, datatypes):
        try:
            self.warehouse_items["tables"] = self.warehouse_items["tables"] + [table_name]
            with open(f"{config.TEMPLATES_FOLDER}datatypes_map.json", "r") as file:
                datatypes_map = json.load(file)
            query = f"CREATE TABLE {table_name} ({columns[0]} {datatypes_map[datatypes[0]]}"
            for i in range(1, len(columns)):
                query += f", {columns[i]} {datatypes_map[datatypes[i]]}"
            query += ");"
            logging.info(f"create table statement designed for table name {table_name}\n{query}")
            return query
        except Exception as e:
            logging.error(f"Function: design_create_table(), Error making the create table statement for {table_name}.")
            raise

    def create_warehouse_item_fabric(self, sql_query):
        try:
            conn_str = (
                "DRIVER={ODBC Driver 17 for SQL Server};"
                f"SERVER={self.endpoint};"
                f"DATABASE={self.warehouse};"
                "Authentication=ActiveDirectoryPassword;"
                f"UID={SSIS_Fabric.username};"
                f"PWD={SSIS_Fabric.password}"
            )
            with pyodbc.connect(conn_str) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql_query)
                    conn.commit()
                    logging.info("Table/Procedure created successfully.")        
        except Exception as e:
            logging.error(f"Function: create_warehouse_item_fabric(), Error creating table/procedure in Fabric warehouse.")
            raise

    def drop_warehouse_items_fabric(self):
        try:
            conn_str = (
                "DRIVER={ODBC Driver 17 for SQL Server};"
                f"SERVER={self.endpoint};"
                f"DATABASE={self.warehouse};"
                "Authentication=ActiveDirectoryPassword;"
                f"UID={SSIS_Fabric.username};"
                f"PWD={SSIS_Fabric.password}"
            )
            
            with pyodbc.connect(conn_str) as conn:
                with conn.cursor() as cursor:
                    for table in self.warehouse_items["tables"]:
                        drop_table_query = f"DROP TABLE IF EXISTS dbo.{table};"
                        cursor.execute(drop_table_query)
                        logging.info(f"Table '{table}' dropped successfully.")
                    
                    for procedure in self.warehouse_items["procedures"]:
                        drop_procedure_query = f"DROP PROCEDURE IF EXISTS dbo.{procedure};"
                        cursor.execute(drop_procedure_query)
                        logging.info(f"Procedure '{procedure}' dropped successfully.")
                
                    conn.commit()
        except Exception as e:
            logging.error(f"Function: drop_warehouse_items_fabric(), Error dropping tables/procedures.")
            raise

    @staticmethod
    def clean_pipeline():
        with open(f"{config.TEMPLATES_FOLDER}pipeline.json", "r+") as file:
            pipeline = json.load(file)
            pipeline["name"] = ""
            pipeline["properties"]["activities"] = []
            file.seek(0)        
            json.dump(pipeline, file, indent=4)
            file.truncate()
            logging.info("pipeline.json cleaned up")
    
    @staticmethod
    def clean_payload():
        with open(f"{config.TEMPLATES_FOLDER}payload.json", "r+") as file:
            pipeline = json.load(file)
            pipeline["displayName"] = ""
            pipeline["definition"]["parts"][0]["payload"] = ""
            file.seek(0)        
            json.dump(pipeline, file, indent=4)
            file.truncate()
            logging.info("payload.json cleaned up")

    @staticmethod
    def encode_json_to_base64():
        try:
            with open(f"{config.TEMPLATES_FOLDER}pipeline.json", "rb") as file:
                file_content = file.read()
                base64_encoded_data = base64.b64encode(file_content)
                base64_string = base64_encoded_data.decode("utf-8")
                logging.info("Encoded pipeline.json into base64 format")
            
            SSIS_Fabric.clean_pipeline()
            return base64_string
        except Exception as e:
            logging.error(f"Function: encode_json_to_base64(), Error while encoding pipeline.json")
            raise

    @staticmethod
    def create_payload_json(pipeline_name, encoded_json): # creates the payload that is pushed to fabric via API
        try:
            with open(f"{config.TEMPLATES_FOLDER}payload.json", "r+") as file:
                pipeline = json.load(file)
                pipeline["displayName"] = pipeline_name
                pipeline["definition"]["parts"][0]["payload"] = encoded_json
                logging.info("payload created for API request")
                file.seek(0)        
                json.dump(pipeline, file, indent=4)
                file.truncate()
        except Exception as e:
            logging.error(f"Function: create_payload_json(), Error in creating payload.json")
            raise

    def create_pipeline(self):
        try:
            with open(f"{config.TEMPLATES_FOLDER}payload.json", "r+") as file:
                pipeline = json.load(file)
                headers = {
                    "Authorization": f"Bearer {self.access_token}",
                    "Content-Type": "application/json"
                }
                
                pipeline_payload = json.dumps(pipeline)  # Convert the loaded JSON into a string
                response = requests.post(f"{SSIS_Fabric.api_base_url}workspaces/{self.workspace_id}/items", headers=headers, data=pipeline_payload)
                
                pipeline["displayName"] = ""
                pipeline["definition"]["parts"][0]["payload"] = ""
                file.seek(0)        
                json.dump(pipeline, file, indent=4)
                file.truncate()
                logging.info("payload.json cleaned up")
                if response.status_code == 201:
                    logging.info(f"Pipeline created successfully\n{response.json()}")
                    return response.json()
                else:
                    logging.info(f"Failed to create pipeline\n{response.json()}")
                    raise Exception(f"Failed to create pipeline. Status code: {response.status_code}, Response: {response.json()}")
        except Exception as e:
            logging.error(f"Function create_pipeline(), Error creating the pipeline")
            raise
            
    # initializes the maps
    def parse_dataflow(self, dataflow, name):
        try:
            logging.info(f"Parsing the executable {name}")
            components = dataflow.xpath(f"DTS:ObjectData/pipeline/components/component", namespaces=SSIS_Fabric.namespaces)
            for component in components:
                component_class = component.xpath("@componentClassID")[0]
                component_name = component.xpath("@name")[0]
                self.component_map[component_name] = [component_class, False]
                if "Source" in component_class:
                    table, columns, datatypes = SSIS_Fabric.parse_source_component(dataflow, component_name)
                    self.component_map[component_name] = self.component_map[component_name] + [table]
                self.flows[component_name] = []
                self.dependency_map[component_name] = []
            logging.info(f"Component map : {self.component_map}")
        except Exception as e:
            logging.error(f"Function: parse_dataflow(), Failed to create the component map.")
            raise

        try:
            data_paths = dataflow.xpath("DTS:ObjectData/pipeline/paths/path", namespaces=SSIS_Fabric.namespaces)
            for path in data_paths:
                source_id = path.xpath("@startId")[0]
                destination_id = path.xpath("@endId")[0]
                start_comp = source_id[source_id.find("\\")+1: source_id.find(".")].split("\\")[1]
                end_comp = destination_id[destination_id.find("\\")+1: destination_id.find(".")].split("\\")[1]

                self.flows[start_comp] = self.flows[start_comp] + [[end_comp, self.component_map[end_comp][0]]]
                self.dependency_map[end_comp] = self.dependency_map[end_comp] + [start_comp]
            logging.info(f"Execution Flow Map: {self.flows}")
            logging.info(f"Dependency map: {self.dependency_map}")
        except Exception as e:
            logging.error(f"Function: parse_dataflow(), Failed to create flow map and dependency map.")
            raise

    # driving function 2
    def parse_components(self, dataflow, dataflow_name, prev_executable):
        count = 0
        completed = []
        try:
            while count < len(self.component_map.keys()):
                for name, comp_type in self.component_map.items():
                    if self.component_map[name][1] == True:
                        logging.info(f"Component {name} is parsed")
                        if name not in completed:
                            completed = completed + [name]
                        count = len(completed)

                    elif "Source" in self.component_map[name][0]:
                        next_comp_type = self.flows[name][0][1]
                        next_comp_name = self.flows[name][0][0]
                        activity_name = f"CopyActivity{self.counts["copy"]}"
                        if (next_comp_type == "Microsoft.Sort" and self.flows[next_comp_name][0][1] == "Microsoft.MergeJoin"):
                            table_name, source_cols, types = SSIS_Fabric.parse_source_component(dataflow, name) # get source table name
                            source_columns, datatypes = SSIS_Fabric.get_columns_from_sort(dataflow, next_comp_name)
                            query = self.design_create_table(table_name, source_columns, datatypes)
                            self.create_warehouse_item_fabric(query)
                            self.copy_activity_json("dbo", table_name, activity_name, source_columns, prev_executable)
                        elif next_comp_type == "Microsoft.Lookup":
                            table_name, source_columns, datatypes = SSIS_Fabric.parse_source_component(dataflow, name)
                            query = self.design_create_table(table_name, source_columns, datatypes)
                            self.create_warehouse_item_fabric(query)
                            self.copy_activity_json("dbo", table_name, activity_name, source_columns, prev_executable)
                        elif "Destination" in next_comp_name:
                            table, source_columns, types = SSIS_Fabric.parse_source_component(dataflow, name) # get source table name
                            table_name, dest_columns, datatypes = SSIS_Fabric.parse_destination_component(dataflow, next_comp_name)
                            query = self.design_create_table(table_name, dest_columns, types)
                            self.create_warehouse_item_fabric(query)
                            self.copy_activity_json("dbo", table_name, activity_name, dest_columns, prev_executable)
                            self.executables[dataflow_name] += [activity_name]
                        self.component_map[name] = self.component_map[name] + [activity_name] # add activity name
                        self.component_map[name][1] = True
                        self.counts["copy"] += 1
                    
                    elif self.component_map[name][0] == "Microsoft.Sort":
                        if self.component_map[self.dependency_map[name][0]][1] == True:
                            self.component_map[name][1] = True
                            self.component_map[name] = self.component_map[name] + [self.component_map[self.dependency_map[name][0]][2]] # add output table name to component_map
                            self.component_map[name] = self.component_map[name] + [self.component_map[self.dependency_map[name][0]][3]]
                    
                    elif self.component_map[name][0] == "Microsoft.MergeJoin":
                        d1 = self.dependency_map[name][0]
                        d2 = self.dependency_map[name][1]
                        if self.component_map[d1][1] and self.component_map[d2][1]:
                            t1 = self.component_map[d1][2]
                            t2 = self.component_map[d2][2]
                            query = SSIS_Fabric.parse_merge(dataflow, name, t1, t2)
                            activity_name = f"StoredProcedure{self.counts["procedure"]}"
                            dest_table = ""
                            if "Destination" in self.flows[name][0][1]:
                                dest_table, _, _ = SSIS_Fabric.parse_destination_component(dataflow, self.flows[name][0][0])
                                self.executables[dataflow_name] += [activity_name]
                            else:
                                self.component_map[name] = self.component_map[name] + [f"{t1}_{t2}"] # add output table name to component_map
                                dest_table = f"{t1}_{t2}"
                            query = query.replace("schema", "dbo")
                            procedure_name = f"Merge_{dest_table}"
                            procedure = self.design_create_procedure(procedure_name, dest_table, query)
                            logging.info("Procedure: ", procedure)
                            self.create_warehouse_item_fabric(procedure)
                            activity1 = self.component_map[d1][3]
                            activity2 = self.component_map[d2][3]
                            self.procedure_json(procedure_name, activity_name, [activity1, activity2])

                            self.component_map[name] += [activity_name]
                            self.component_map[name][1] = True
                            self.counts["procedure"] += 1
                    
                    elif self.component_map[name][0] == "Microsoft.Lookup":
                        dependency = self.dependency_map[name][0]
                        if self.component_map[dependency][1] == True:
                            t1 = self.component_map[dependency][2]
                            columns = self.get_columns_for_lookup(dataflow, dependency)
                            t2, query, ref_columns, datatypes = self.parse_lookup(dataflow, name, t1, columns)
                            table_query = self.design_create_table(t2, ref_columns, datatypes)
                            self.create_warehouse_item_fabric(table_query)
                            copy_name = f"CopyActivity{self.counts["copy"]}"
                            self.copy_activity_json("dbo", t2, copy_name, ref_columns, prev_executable)
                            self.counts["copy"] += 1
                            activity_name = f"StoredProcedure{self.counts["procedure"]}"
                            dest_table = ""
                            if "Destination" in self.flows[name][0][1]:
                                dest_table, _, _ = SSIS_Fabric.parse_destination_component(dataflow, self.flows[name][0][0])
                                self.executables[dataflow_name] += [activity_name]
                            else:
                                self.component_map[name] = self.component_map[name] + [f"{t1}_{t2}"] # add output table name to component_map
                                dest_table = f"{t1}_{t2}"
                            query = query.replace("schema", "dbo")
                            procedure_name = f"Lookup_{dest_table}"
                            procedure = self.design_create_procedure(procedure_name, dest_table, query)
                            self.create_warehouse_item_fabric(procedure)
                            logging.info("Procedure: ", procedure)
                            activity1 = self.component_map[dependency][3]
                            activity2 = copy_name
                            self.procedure_json(procedure_name, activity_name, [activity1, activity2])
                            self.component_map[name] += [activity_name]
                            self.component_map[name][1] = True
                            self.counts["procedure"] += 1

                    else:
                        if self.component_map[self.dependency_map[name][0]][1] == True:
                            self.component_map[name][1] = True
            logging.info(f"Resultant component map {self.component_map}")
            self.component_map = {}
            self.dependency_map = {}
            self.flows = {}
        except Exception as e:
            logging(f"Function: parse_components(), Error in parsing the components.")
            raise

    #driving function 1
    def parse_ssis_pipeline(self, filepath):
        try:
            tree = etree.parse(filepath)
            pipeline_executables = tree.xpath("//DTS:Executables/DTS:Executable", namespaces=SSIS_Fabric.namespaces)
            executables_names = tree.xpath("//DTS:Executables/DTS:Executable/@DTS:ObjectName", namespaces=SSIS_Fabric.namespaces)
            logging.info(f"Executables: {executables_names}")
            n = len(pipeline_executables)

            for i in range(n):
                exec_type = pipeline_executables[i].xpath("@DTS:ExecutableType", namespaces=SSIS_Fabric.namespaces)[0]
                name = pipeline_executables[i].xpath("@DTS:ObjectName", namespaces=SSIS_Fabric.namespaces)[0]
                prev_executable = executables_names[i-1] if i > 0 else None
            
                logging.info(f"Executable {name} is a {exec_type}")
                self.executables[name] = []
                if exec_type == "Microsoft.Pipeline":
                    self.parse_dataflow(pipeline_executables[i], name)
                    self.parse_components(pipeline_executables[i], name, prev_executable)
                elif exec_type == "Microsoft.ExecuteSQLTask":
                    procedure_name = SSIS_Fabric.parse_execsql(pipeline_executables[i], name)
                    self.counts["procedure"] += 1
                    activity_name = f"StoredProcedure{self.counts["procedure"]}"
                    self.executables[name] += [activity_name]
                    self.procedure_json(procedure_name, activity_name, self.executables[executables_names[i-1]])
                    logging.info(f"Procedure used in Execute SQL is {procedure_name}")
                logging.info(f"Executables last activitites map: {self.executables}")

            with open(f"{config.TEMPLATES_FOLDER}pipeline.json", "r+") as file:
                pipeline = json.load(file)
                pipeline["name"] = self.pipeline_name
                logging.info(f"Added pipeline name to pipeline.json")
                file.seek(0)        
                json.dump(pipeline, file, indent=4)
                file.truncate()
        except Exception as e:
            logging.error(f"Function: parse_dtsx_pipeline(), Error parsing the .dtsx file.")
            raise