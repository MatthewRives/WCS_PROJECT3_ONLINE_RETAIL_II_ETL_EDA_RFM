"""
=============================================================
Function: Export data to xslx
=============================================================
Script purpose:
    ...

Process:
    01. ...
    End of process

List of functions used: 
    - ...

Potential improvements: 
    - Not determined yet

    
Example of usage:
dict_data_to_export = {
    "DataFrame Example": df_example,
    "List Example": list_example,
    "Graph Example": fig_example
}
fx_export_data_to_excel(dict_data_to_export, "example_export", "data_exploration")
   
WARNING:
    ...
"""

# 1. Import librairies ----
import os
import re
import xlsxwriter
import pandas as pd
import matplotlib.axes as maxes
import matplotlib.figure as mfig
import seaborn as sns

DATA_PATH = "/opt/airflow/data"

# 2. Create export data to excel function ----
def fx_export_data_to_excel(dict_data_to_export, file_name: str, folder_in_data="data_exploration"):
    try:
        print(f"\n########### Exporting to Excel file ###########")

        # Fixed container path instead of relative path
        folder_path = os.path.join(DATA_PATH, folder_in_data)
        os.makedirs(folder_path, exist_ok=True)

        file_name = re.sub(r'\W+', '_', file_name.lower())
        file_name = f"{file_name}.xlsx"
        full_file_path = os.path.join(folder_path, file_name)

        print(f"\n- Exporting to: {full_file_path} -")

        writer = pd.ExcelWriter(full_file_path, engine='xlsxwriter')
        temp_images = []

        with writer:
            workbook = writer.book
            number_of_sheets = len(dict_data_to_export)
            count = 0

            print(f"\n───── Exporting each sheet ─────")
            for data_name, data_content in dict_data_to_export.items():
                count += 1
                print(f"\n- Treating content {count}/{number_of_sheets}: {data_name} -")

                clean_name = re.sub(r'\W+', '_', data_name.upper())
                sheet_name = clean_name[:31]
                print(f"  Sheet name: {sheet_name}")

                if isinstance(data_content, pd.DataFrame):
                    data_type = "dataframe"
                elif isinstance(data_content, list):
                    data_type = "list"
                elif isinstance(data_content, (maxes.Axes, sns.axisgrid.FacetGrid, mfig.Figure)):
                    data_type = "graph"
                else:
                    print(f"  Warning: unknown type for {sheet_name}, skipping.")
                    continue

                print(f"  Data type: {data_type}")
                max_excel_rows = 1048575

                if data_type in ("dataframe", "list"):
                    if data_type == "list":
                        data_content = pd.DataFrame(data_content, columns=["VALUES"])
                    if len(data_content) > max_excel_rows:
                        print(f"  Warning: truncating to {max_excel_rows} rows")
                        data_content = data_content.head(max_excel_rows)

                    data_content.to_excel(writer, sheet_name=sheet_name,
                                          startrow=1, header=False, index=False)
                    worksheet = writer.sheets[sheet_name]
                    max_row, max_col = data_content.shape
                    column_settings = [{"header": col} for col in data_content.columns]
                    worksheet.add_table(0, 0, max_row, max_col - 1,
                                        {"columns": column_settings})
                    worksheet.autofit()

                elif data_type == "graph":
                    worksheet = workbook.add_worksheet(sheet_name)
                    temp_img = os.path.join(folder_path, f"temp_{sheet_name}.png")
                    data_content.savefig(temp_img, bbox_inches='tight', dpi=150)
                    worksheet.insert_image('A1', temp_img)
                    temp_images.append(temp_img)

        print(f"\n########### Clean temporary images ###########")
        for temp_img in temp_images:
            if os.path.exists(temp_img):
                os.remove(temp_img)

        print(f"\n########### End of export ###########")
        print(f"File saved: {full_file_path}")

    except Exception as error:
        print(f"Error: {error}")
        import traceback
        traceback.print_exc()
        
        
        
        


