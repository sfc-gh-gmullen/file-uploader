import streamlit as st
import pandas as pd
import json
###################################
from st_aggrid import AgGrid
from st_aggrid.grid_options_builder import GridOptionsBuilder
from st_aggrid.shared import JsCode 
###################################
#from functionforDownloadButtons import upload_button
from snowflake.snowpark.session import Session
from snowflake.snowpark import functions as F
from snowflake.snowpark.types import *
from snowflake.snowpark import Window

if 'connection_parameters' not in st.session_state:
    st.error('Set Context first!')
else:
        
    CONNECTION_PARAMETERS = st.session_state.connection_parameters 
    session = Session.builder.configs(CONNECTION_PARAMETERS).create()

    st.set_page_config(page_icon="✂️", page_title="Auto Table Loader")

    st.title("File Uploader")

    uploaded_file = st.file_uploader(
        "",
        key="1"
    )

    if uploaded_file is not None:
        file_container = st.expander("Check your uploaded .csv")
        shows = pd.read_csv(uploaded_file,quotechar='\"',quoting=1,doublequote=True)
        #suploaded_file.seek(0)
        file_container.write(shows)
        target = st.text_input("Table Name",uploaded_file.name.rsplit( ".", 1 )[ 0 ])
        upload_to_snowflake = st.button("Upload to Snowflake")
    else:
        st.info(
            f"""
                👆 Upload a .csv file first. Sample to try: [biostats.csv](https://people.sc.fsu.edu/~jburkardt/data/csv/biostats.csv)
                """
        )
        st.stop()


    if upload_to_snowflake and target:
        # upload here
        df = session.write_pandas(shows,target,auto_create_table=True,quote_identifiers=False)
        #df.write.mode("overwrite").save_as_table(target)

