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

import xlsxwriter
import re 
import pandas as pd
import matplotlib.axes as maxes
import matplotlib.figure as mfig
import seaborn as sns


# 2. Create export data to excel function ----
def fx_export_data_to_excel(dict_data_to_export, file_name: str, folder_in_data="data_exploration"):
    try: 
        print(f"\n########### Exporting to Excel file ###########")
        print(f"\n---------- Defining path ----------")
        print(f"\n- Get the directory where this script is located -")
        script_dir = os.path.dirname(os.path.abspath(__file__))

        print(f"\n- Building absolute path for data exploration results folder -")
        folder_path = os.path.join(script_dir, "..", "data", folder_in_data)
        folder_path = os.path.abspath(folder_path)

        print(f"\n- Creating folder if needed at: {folder_path} -")
        os.makedirs(folder_path, exist_ok=True)

        print(f"\n- Defining file name -")
        file_name = re.sub(r'\W+', '_', file_name.lower())
        file_name = f"{file_name}.xlsx"

        print(f"\n- Defining full file path -")
        full_file_path = os.path.join(folder_path, file_name)

        print(f"\n- Defining writer -")
        writer = pd.ExcelWriter(full_file_path, engine='xlsxwriter')

        print(f"\n- Creating temporary images list for graph if needed -")
        temp_images = [] 

        print(f"\n- Defining workbook -")
        with writer:
            workbook = writer.book

            number_of_sheets = len(dict_data_to_export)
            count = 0

            print(f"\n---------- Exporting each sheet ----------")
            for data_name, data_content in dict_data_to_export.items():
                count += 1
                print(f"\n- Treating content {count}/{number_of_sheets}: {data_name} -")

                # ---
                print(f"    Defining sheet name:")
                clean_name = re.sub(r'\W+', '_', data_name.upper())
                sheet_name = clean_name[:31] # Excel sheet names are limited to 31 characters
                print(f"        {sheet_name}")


                # ---
                print(f"    Defining data type:")
                if isinstance(data_content, pd.DataFrame):
                    data_type = "dataframe"
                    print(f"        {data_type}")
                elif isinstance(data_content, list):
                    data_type = "list"
                    print(f"        {data_type}")
                elif isinstance(data_content, (maxes.Axes, sns.axisgrid.FacetGrid, mfig.Figure)):
                    data_type = "graph"
                    print(f"        {data_type}")
                else:
                    data_type = "unknown"
                    print(f"        Warning: data type for {sheet_name} is unknown.")
                    print(f"        DEBUG: type(data_content) = {type(data_content)}")
                    print(f"        DEBUG: data_content.__class__.__module__ = {data_content.__class__.__module__}")
                    print("     Skipping export for this content.")
                    continue

                # ---
                print(f"    Exporting content according to data type")
                max_excel_rows = 1048575

                if data_type == "dataframe":
                    if len(data_content) > max_excel_rows:
                        print(f"    Warning: {sheet_name} too big ({len(data_content)} rows)")
                        print(f"    Shortening it to the first {max_excel_rows} rows")
                        data_content = data_content.head(max_excel_rows)
                    # data_content.to_excel(writer, sheet_name=sheet_name, index=False)

                    data_content.to_excel(writer, sheet_name=sheet_name, startrow=1, header=False, index=False)
                    worksheet = writer.sheets[sheet_name]
                    max_row, max_col = data_content.shape
                    column_settings = [{"header": column} for column in data_content.columns]
                    worksheet.add_table(0, 0, max_row, max_col - 1, {"columns": column_settings})
                    # worksheet.set_column(0, max_col - 1, 12)
                    worksheet.autofit()


                elif data_type == "list":
                    data_content = pd.DataFrame(data_content, columns=["VALUES"])
                    if len(data_content) > max_excel_rows:
                        print(f"    Warning: {sheet_name} too big ({len(data_content)} rows)")
                        print(f"    Shortening it to the first {max_excel_rows} rows")
                        data_content = data_content.head(max_excel_rows)
                    # data_content.to_excel(writer, sheet_name=sheet_name, index=False)

                    data_content.to_excel(writer, sheet_name=sheet_name, startrow=1, header=False, index=False)
                    worksheet = writer.sheets[sheet_name]
                    max_row, max_col = data_content.shape
                    column_settings = [{"header": column} for column in data_content.columns]
                    worksheet.add_table(0, 0, max_row, max_col - 1, {"columns": column_settings})
                    # worksheet.set_column(0, max_col - 1, 12)
                    worksheet.autofit()



                elif data_type == "graph":
                    worksheet = workbook.add_worksheet(sheet_name)
                    temp_img = os.path.join(folder_path, f"temp_{sheet_name}.png")
                    data_content.savefig(temp_img, bbox_inches='tight', dpi=150)
                    worksheet.insert_image('A1', temp_img)
                    temp_images.append(temp_img)

        # ---
        print(f"\n########### Clean temporary images ###########")
        for temp_img in temp_images:
            if os.path.exists(temp_img):
                os.remove(temp_img)

        # ---
        print(f"\n########### End of export to Excel ###########")
        print(f"File {file_name} saved to {full_file_path}")

    except Exception as error:
        print(f"Error: {error}")
        import traceback
        traceback.print_exc()

