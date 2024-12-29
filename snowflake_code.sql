-- Create an internal stage with Directory enables
-- Having Directory enabled lets us query the un-structured data in the stage.
CREATE OR REPLACE STAGE SANDBOX.MBR_EDP_DATA_MANAGEMENT.apsn_settlement_docs
  DIRECTORY = (ENABLE = TRUE);
LIST @SANDBOX.MBR_EDP_DATA_MANAGEMENT.apsn_settlement_docs;
REMOVE @SANDBOX.MBR_EDP_DATA_MANAGEMENT.apsn_settlement_docs;

-- Python UDF to read pdf data from staged PDF files
CREATE OR REPLACE FUNCTION SANDBOX.MBR_EDP_DATA_MANAGEMENT.get_customer_settlement_info_from_pdf(file string)
    returns string
    language python 
    runtime_version=3.8
    packages = ('snowflake-snowpark-python', 'PyPDF2')
    handler = 'read_file'
as
$$
from PyPDF2 import PdfReader
from snowflake.snowpark.files import SnowflakeFile

def read_file(file_path):
    with SnowflakeFile.open(file_path, 'rb') as file:
        pdf_reader = PdfReader(file)
        page_num = int(len(pdf_reader.pages))-1
        page = pdf_reader.pages[page_num]
        text = page.extract_text()
        lines = text.split('\n')
        # grabbing the signed name from the pdf
        signed_name = lines[2].split(':')[1].split('Date')[0].strip()
        # grabbing the timestamp 
        timestamp = lines[-1].split(':')[1].split('T')[0].strip()
        # creating a list
        data = str([signed_name, timestamp])
    return data
        
$$;

-- Manually refresh the directory table on the stage to view the files uploaded using PUT command.
ALTER STAGE SANDBOX.MBR_EDP_DATA_MANAGEMENT.apsn_settlement_docs REFRESH;

-- Directory table to store the stage file data. It incorporates the Python UDF to parse out specific data from the files. 
CREATE OR REPLACE TABLE SANDBOX.MBR_EDP_DATA_MANAGEMENT.T_PARSED_PDF AS (
    SELECT 
        relative_path,
        file_url,
        -- using the build_scoped_file_url function to generates a scoped Snowflake file URL to a staged file using the stage name and relative file path as inputs.
        SANDBOX.MBR_EDP_DATA_MANAGEMENT.get_customer_settlement_info_from_pdf(build_scoped_file_url(@SANDBOX.MBR_EDP_DATA_MANAGEMENT.apsn_settlement_docs, relative_path)) as parsed_data
    FROM DIRECTORY(@SANDBOX.MBR_EDP_DATA_MANAGEMENT.apsn_settlement_docs)
);

-- Final query
SELECT *
FROM SANDBOX.MBR_EDP_DATA_MANAGEMENT.T_PARSED_PDF;