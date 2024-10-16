from flask import Flask, request, jsonify
import os
import traceback
import logging
from pathlib import Path
from ssis_fabric import SSIS_Fabric
import config

parent_directory = Path(__file__).parent.parent
log_file_path = parent_directory / config.LOG_FILE
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path),
        logging.StreamHandler()
    ]
)

app = Flask(__name__)

@app.route('/migrate', methods=['POST'])
def migrate():
    try:
        dtsx_file = request.files['dtsxFile']
        workspace_name = request.form['workspaceName']
        lakehouse_name = request.form['lakehouseName']
        warehouse_name = request.form['warehouseName']
        pipeline_name = request.form['pipelineName']
        endpoint = request.form['endpoint']

        logging.info(f"File received is {dtsx_file.filename}")
        parent_dir = Path(__file__).resolve().parent.parent
        temp_dir = parent_dir / config.UPLOAD_FOLDER
        
        dtsx_file_path = os.path.join(temp_dir, dtsx_file.filename)
        dtsx_file.save(dtsx_file_path)
        logging.info("File saved in temp library")

        obj = SSIS_Fabric(workspace_name, lakehouse_name, warehouse_name, pipeline_name, endpoint)
        obj.create_token()
        obj.get_workspace_id()
        obj.get_lakehouse_id()
        obj.get_warehouse_id()
        obj.parse_ssis_pipeline(dtsx_file_path)

        encoded = obj.encode_json_to_base64()
        obj.create_payload_json(pipeline_name, encoded)
        response = obj.create_pipeline()

        if os.path.exists(dtsx_file_path):
            os.remove(dtsx_file_path)
        return response

    except Exception as e:
        logging.error(f"Failed to migrate: {str(e)}")
        if os.path.exists(dtsx_file_path):
            os.remove(dtsx_file_path)
        
        # clean up changes made till now
        obj.drop_warehouse_items_fabric()
        SSIS_Fabric.clean_pipeline()
        SSIS_Fabric.clean_payload()
        
        return jsonify({"message": f"Failed to migrate: {str(e)}"}), 500

if __name__ == '__main__':
    app.run(port=config.PORT, debug=True)