import streamlit as st
import pandas as pd
import numpy as np
import json
import re
import io
import base64
from datetime import datetime
from typing import Dict, List, Any, Union, Optional

# Set page£®UI ) configuration
st.set_page_config(
    page_title="CSV Data Retrieval and Conversion Tool-China mobile",
    page_icon="??",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS styles for UI
def load_css():
    st.markdown("""
    <style>
        .stApp {
            background-color: #f0f2f6;
        }
        .stHeader {
            color: #2c3e50;
        }
        .stButton>button {
            background-color: #4e73df;
            color: white;
            border-radius: 5px;
            padding: 0.5rem 1rem;
            font-weight: 600;
        }
        .stButton>button:hover {
            background-color: #2e59d9;
            color: white;
        }
        .stSelectbox>div>div>div {
            background-color: white;
        }
        .stTextInput>div>div>input {
            background-color: white;
        }
        .stNumberInput>div>div>input {
            background-color: white;
        }
        .stCheckbox>div>div {
            background-color: white;
        }
        .stRadio>div {
            background-color: white;
            padding: 10px;
            border-radius: 5px;
        }
        .stSuccess {
            color: #1cc88a;
            font-weight: 600;
        }
        .stWarning {
            color: #f6c23e;
            font-weight: 600;
        }
        .stError {
            color: #e74a3b;
            font-weight: 600;
        }
        .config-box {
            background-color: white;
            border-radius: 10px;
            padding: 20px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            margin-bottom: 20px;
        }
        .result-box {
            background-color: white;
            border-radius: 10px;
            padding: 20px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            margin-top: 20px;
        }
        .section-title {
            color: #2c3e50;
            border-bottom: 2px solid #4e73df;
            padding-bottom: 10px;
            margin-bottom: 15px;
        }
        .footer {
            text-align: center;
            padding: 20px;
            color: #6c757d;
            font-size: 0.9rem;
            margin-top: 30px;
        }
    </style>
    """, unsafe_allow_html=True)

# the class of Main application processing 
class CSVDataProcessor:
    def __init__(self):
        self.df = None
        self.config = {
            "selected_columns": [],
            "filters": [],
            "sorting": [],
            "grouping": None,
            "aggregations": []
        }
        self.processed_df = None
        self.config_history = []
    
    def load_csv(self, file):
        #"""Load CSV file"""
        try:
            self.df = pd.read_csv(file)
            return True, f"Successfully loaded CSV file, with {len (self. df)} rows and {len (self. df. columns)} columns"
        except Exception as e:
            return False, f"Failed to load CSV file: {str (e)}"
    
    def get_column_names(self):
        #"""Get column names"""
        return list(self.df.columns) if self.df is not None else []
    
    def get_column_dtypes(self):
        #"""Get data type of column """
        if self.df is not None:
            return {col: str(self.df[col].dtype) for col in self.df.columns}
        return {}
    
    def update_config(self, config):
        #"""Update Configuration"""
        self.config = config
        self.config_history.append(config.copy())
    
    def process_data(self):
        #"""Process data according to configuration"""
        if self.df is None:
            return False, "Please upload CSV file first"
        
        try:
            # Copy original data
            result_df = self.df.copy()
            
            # 1. Application column selection
            if self.config['selected_columns']:
                result_df = result_df[self.config['selected_columns']]
            
            # 2. Apply filtering conditions
            for filter_rule in self.config['filters']:
                col = filter_rule['column']
                operator = filter_rule['operator']
                value = filter_rule['value']
                
                # Attempt to convert value type
                try:
                    if result_df[col].dtype == 'float64' or result_df[col].dtype == 'int64':
                        value = float(value)
                except:
                    pass
                
                if operator == 'eq':
                    result_df = result_df[result_df[col] == value]
                elif operator == 'ne':
                    result_df = result_df[result_df[col] != value]
                elif operator == 'gt':
                    result_df = result_df[result_df[col] > value]
                elif operator == 'ge':
                    result_df = result_df[result_df[col] >= value]
                elif operator == 'lt':
                    result_df = result_df[result_df[col] < value]
                elif operator == 'le':
                    result_df = result_df[result_df[col] <= value]
                elif operator == 'contains':
                    result_df = result_df[result_df[col].astype(str).str.contains(str(value), na=False)]
                elif operator == 'regex':
                    try:
                        result_df = result_df[result_df[col].astype(str).str.match(str(value), na=False)]
                    except re.error:
                        st.error(f"Regular expression error: {value}")
                        return False, "Regular expression syntax error"
                elif operator == 'isnull':
                    result_df = result_df[result_df[col].isna()]
                elif operator == 'notnull':
                    result_df = result_df[result_df[col].notna()]
                elif operator == 'in':
                    values = [v.strip() for v in value.split(',')]
                    result_df = result_df[result_df[col].isin(values)]
            
            # 3. Apply sorting rules
            if self.config['sorting']:
                sort_columns = []
                ascending = []
                for sort_rule in self.config['sorting']:
                    sort_columns.append(sort_rule['column'])
                    ascending.append(sort_rule['ascending'])
                result_df = result_df.sort_values(by=sort_columns, ascending=ascending)
            
            # 4. Application group aggregation (optional)
            if self.config['grouping'] and self.config['grouping']['enabled']:
                group_columns = self.config['grouping']['columns']
                agg_config = self.config['aggregations']
                
                # Build Aggregation Configuration
                agg_dict = {}
                for agg in agg_config:
                    col = agg['column']
                    func = agg['function']
                    if col not in agg_dict:
                        agg_dict[col] = []
                    agg_dict[col].append(func)
                
                # Perform group aggregation
                result_df = result_df.groupby(group_columns).agg(agg_dict).reset_index()
                
                # Flattening multi-level column names
                result_df.columns = ['_'.join(col).strip() for col in result_df.columns.values]
            
            self.processed_df = result_df
            return True, f"Data processing completed! The result contains {len (resultd_df)} rows and {len (resultd_df. columns)} columns"
        except Exception as e:
            return False, f"Error occurred during data processing: {str(e)}"
    
    def export_to_csv(self):
        #"""Export as CSV"""
        if self.processed_df is None:
            return None
        return self.processed_df.to_csv(index=False).encode('utf-8')
    
    def export_to_json(self):
        #"""Export as JSON"""
        if self.processed_df is None:
            return None
        return self.processed_df.to_json(orient='records', indent=2).encode('utf-8')
    
    def get_config_json(self):
        #"""Get JSON representation of configuration"""
        return json.dumps(self.config, indent=2)
    
    def load_config_from_json(self, json_str):
        #"""Load configuration from JSON"""
        try:
            self.config = json.loads(json_str)
            return True, "Configuration loaded successfully!"
        except Exception as e:
            return False, f"Configuration loading failed: {str(e)}"

# CVS/json File Download Function
def get_download_link(data, filename, file_type):
    #"""Generate Download Link"""
    b64 = base64.b64encode(data).decode()
    if file_type == 'csv':
        href = f'<a href="data:file/csv;base64,{b64}" download="{filename}">Download CSV file</a>'
    elif file_type == 'json':
        href = f'<a href="data:application/json;base64,{b64}" download="{filename}">Download JSON file</a>'
    else:
        href = f'<a href="data:application/octet-stream;base64,{b64}" download="{filename}">Download file</a>'
    return href

# Main processing-start
def main():
    # Loading UI CSS Styles
    load_css()
    
    # Initialize processor
    if 'processor' not in st.session_state:
        st.session_state.processor = CSVDataProcessor()
    
    processor = st.session_state.processor
    
    # TITLE of UI page
    st.title("?? CSV Data Retrieval and Conversion Tool-China Mobile")
    st.markdown("""
    <div style="color: #6c757d; margin-bottom: 30px;">
        Upload CSV file, configure search rules, and export processed CSV or JSON file. Support functions such as field selection, filtering criteria, regular expressions, sorting rules, and group aggregation°£
    </div>
    """, unsafe_allow_html=True)
    
    # Sidebar - File Upload
    with st.sidebar:
        st.subheader("?? Upload CSV file")
        uploaded_file = st.file_uploader("Select CSV file", type=["csv"])
        
        if uploaded_file is not None:
            success, message = processor.load_csv(uploaded_file)
            if success:
                st.success(message)
                
                # Display data preview
                st.subheader("Data Preview")
                st.dataframe(processor.df.head(10), height=300)
            else:
                st.error(message)
        
        st.markdown("---")
        st.subheader("?? configuration management of Retrieval filtering ")
        
        # Export Configuration
        config_json = processor.get_config_json()
        st.download_button(
            label="Export Retrieval filtering configuration as JSON",
            data=config_json,
            file_name="data_config.json",
            mime="application/json"
        )
        
        # Import configuration
        uploaded_config = st.file_uploader("Import configuration JSON", type=["json"])
        if uploaded_config is not None:
            config_str = uploaded_config.getvalue().decode("utf-8")
            success, message = processor.load_config_from_json(config_str)
            if success:
                st.success(message)
                st.session_state.config_updated = True
            else:
                st.error(message)
        
        st.markdown("---")
        st.info("""
        **Instructions for use:**
          1. Upload CSV file
          2. Configure search rules below
          3. Click on "Apply Configuration and Process Data"
          4. Download the processed file in the results area
        """)
    
    # Main content area
    if processor.df is not None:
        # Configuration Area
        st.subheader("?? Data retrieval configuration")
        
        with st.expander("configuration option", expanded=True):
            col1, col2 = st.columns(2)
            
            # select column
            with col1:
                st.markdown('<div class="config-box">', unsafe_allow_html=True)
                st.markdown('<h4 class="section-title">field selection</h4>', unsafe_allow_html=True)
                all_columns = processor.get_column_names()
                selected_columns = st.multiselect(
                    "Select the columns to include (leaving blank indicates selecting all columns)",
                    all_columns,
                    default=processor.config.get('selected_columns', [])
                )
                st.markdown('</div>', unsafe_allow_html=True)
            
            # sorting rules
            with col2:
                st.markdown('<div class="config-box">', unsafe_allow_html=True)
                st.markdown('<h4 class="section-title">sorting rules</h4>', unsafe_allow_html=True)
                
                sorting_rules = processor.config.get('sorting', [])
                new_sorting_rules = []
                
                for i in range(3):  # Up to 3 sorting rules
                    col3, col4, col5 = st.columns([3, 2, 1])
                    with col3:
                        sort_col = st.selectbox(
                            f"sort field {i+1}",
                            all_columns,
                            index=all_columns.index(sorting_rules[i]['column']) if i < len(sorting_rules) else 0,
                            key=f"sort_col_{i}"
                        )
                    with col4:
                        sort_order = st.selectbox(
                            "sort order",
                            ["Ascending order, descending order"],
                            index=0 if (i >= len(sorting_rules) or sorting_rules[i]['ascending'] else 1,
                            key=f"sort_order_{i}"
                        )
                    with col5:
                        st.write("")
                        if st.button("?", key=f"remove_sort_{i}"):
                            continue
                    
                    if sort_col:  # Only add non empty sorting rules
                        new_sorting_rules.append({
                            "column": sort_col,
                            "ascending": sort_order == "Ascending order"
                        })
                
                # Add new  button of sorting rule
                if st.button("+ Add sorting rules"):
                    new_sorting_rules.append({"column": all_columns[0], "ascending": True})
                
                st.markdown('</div>', unsafe_allow_html=True)
            
            # Filter conditions
            st.markdown('<div class="config-box">', unsafe_allow_html=True)
            st.markdown('<h4 class="section-title">Filter conditions</h4>', unsafe_allow_html=True)
            
            filter_rules = processor.config.get('filters', [])
            new_filter_rules = []
            
            for i, rule in enumerate(filter_rules):
                col6, col7, col8, col9 = st.columns([2, 2, 3, 1])
                with col6:
                    filter_col = st.selectbox(
                        "¡Ï”Ú",
                        all_columns,
                        index=all_columns.index(rule['column']),
                        key=f"filter_col_{i}"
                    )
                with col7:
                    filter_op = st.selectbox(
                        "Operation symbol",
                        ["Equal to "," not equal to "," greater than "," greater than or equal to "," less than "," less than or equal to "," contains "," regular matching "," empty "," not empty "," in the list"],
                        index=["eq", "ne", "gt", "ge", "lt", "le", "contains", "regex", "isnull", "notnull", "in"].index(rule['operator']),
                        key=f"filter_op_{i}"
                    )
                with col8:
                    if filter_op in ["Empty, not empty"]:
                        filter_value = ""
                    else:
                        filter_value = st.text_input(
                            "value",
                            value=rule['value'],
                            key=f"filter_value_{i}"
                        )
                with col9:
                    st.write("")
                    if st.button("?", key=f"remove_filter_{i}"):
                        continue
                
                op_mapping = {
                    "equal to": "eq",
                    "Not equal to": "ne",
                    "greater than": "gt",
                    "greater than or equal": "ge",
                    "less than": "lt",
                    "less than or equal": "le",
                    "contain": "contains",
                    "Regular matching": "regex",
                    "For empty": "isnull",
                    "Not empty": "notnull",
                    "in list": "in"
                }
                
                new_filter_rules.append({
                    "column": filter_col,
                    "operator": op_mapping[filter_op],
                    "value": filter_value
                })
            
            # Add New Button for Filter Condition
            if st.button("+ Add filtering conditions"):
                new_filter_rules.append({"column": all_columns[0], "operator": "eq", "value": ""})
            
            st.markdown('</div>', unsafe_allow_html=True)
            
            # Group aggregation (optional)
            st.markdown('<div class="config-box">', unsafe_allow_html=True)
            st.markdown('<h4 class="section-title">Group aggregation</h4>', unsafe_allow_html=True)
            
            grouping_enabled = st.checkbox(
                "Enable group aggregation", 
                value=processor.config.get('grouping', {}).get('enabled', False)
            )
            
            grouping_config = {"enabled": grouping_enabled}
            agg_config = []
            
            if grouping_enabled:
                col10, col11 = st.columns(2)
                
                with col10:
                    group_columns = st.multiselect(
                        "Grouped Fields",
                        all_columns,
                        default=processor.config.get('grouping', {}).get('columns', [])
                    )
                    grouping_config["columns"] = group_columns
                
                with col11:
                    st.write("Aggregate function configuration")
                    
                    agg_rules = processor.config.get('aggregations', [])
                    new_agg_rules = []
                    
                    for i, rule in enumerate(agg_rules):
                        col12, col13, col14 = st.columns([3, 3, 1])
                        with col12:
                            agg_col = st.selectbox(
                                "field",
                                all_columns,
                                index=all_columns.index(rule['column']),
                                key=f"agg_col_{i}"
                            )
                        with col13:
                            agg_func = st.selectbox(
                                "function",
                                ["Sum", "Average", "Count", "Maximum", "Minimum", "Standard Deviation"],
                                index=["sum", "mean", "count", "max", "min", "std"].index(rule['function']),
                                key=f"agg_func_{i}"
                            )
                        with col14:
                            st.write("")
                            if st.button("?", key=f"remove_agg_{i}"):
                                continue
                        
                        func_mapping = {
                            "Sum": "sum",
                            "Average": "mean",
                            "Count": "count",
                            "Maximum": "max",
                            "Minimum": "min",
                            "Standard Deviation": "std"
                        }
                        
                        new_agg_rules.append({
                            "column": agg_col,
                            "function": func_mapping[agg_func]
                        })
                    
                    # Add New Aggregation Rule Button
                    if st.button("+ Add aggregation rules"):
                        new_agg_rules.append({"column": all_columns[0], "function": "sum"})
                    
                    agg_config = new_agg_rules
                
                grouping_config["columns"] = group_columns
            
            st.markdown('</div>', unsafe_allow_html=True)
            
            # Update Configuration
            new_config = {
                "selected_columns": selected_columns,
                "filters": new_filter_rules,
                "sorting": new_sorting_rules,
                "grouping": grouping_config,
                "aggregations": agg_config
            }
            
            processor.update_config(new_config)
        
        # Application Configuration Button
        if st.button("?? Application configuration and data processing", use_container_width=True):
            with st.spinner("Searching and processing data..."):
                success, message = processor.process_data()
                if success:
                    st.success(message)
                else:
                    st.error(message)
        
        # Display configuration JSON
        st.subheader("?? currently allocated")
        st.json(processor.config)
        
        # results area
        if processor.processed_df is not None:
            st.subheader("?? Processing results")
            
            # Display result preview
            st.dataframe(processor.processed_df.head(20), height=500)
            
            # export option
            st.subheader("?? Export Results")
            
            col15, col16 = st.columns(2)
            
            with col15:
                output_filename = st.text_input("output file name", "processed_data")
                export_format = st.radio("export format", ["CSV", "JSON"])
            
            with col16:
                st.write("")
                st.write("")
                if export_format == "CSV":
                    csv_data = processor.export_to_csv()
                    if csv_data:
                        st.download_button(
                            label="Download CSV file",
                            data=csv_data,
                            file_name=f"{output_filename}.csv",
                            mime="text/csv"
                        )
                else:
                    json_data = processor.export_to_json()
                    if json_data:
                        st.download_button(
                            label="Download JSON file",
                            data=json_data,
                            file_name=f"{output_filename}.json",
                            mime="application/json"
                        )
    else:
        st.info("Please upload CSV file to start processing°£")
    
    # footer of UI
    st.markdown("---")
    st.markdown('<div class="footer">CSV Data Retrieval and Conversion Tool(China Mobile | Built with Python and Streamlinet</div>', unsafe_allow_html=True)

if __name__ == "__main__":
    main()
	