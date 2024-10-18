import tkinter as tk
from tkinter import ttk, filedialog
import pandas as pd
from datetime import datetime
import re
import numpy as np
import xlwings as xw
import os
import threading
#Check if the input works with thef olwoo

def process_data():
    source_file = source_entry.get()
    destination_file = destination_entry.get()
    year = int(year_entry.get())
    global column_mapping
    global columns_in_table
    
    
    def process_data_thread(): 
        display_output("Processing started...")

        def process_data_48(dataframe, year_to_find):

            def contains_year(cell, year_to_find):
                return isinstance(cell, datetime) and cell.year == year_to_find

            values_dict_column_b = {}
            values_dict_column_d = {}
            column_b = dataframe.iloc[:, 1].astype(str)
            column_d = dataframe.iloc[:, 3].astype(str)

            year_indices = []
            row_number=26
            for column_index, column in enumerate(dataframe.columns):
                if dataframe[column].apply(contains_year, year_to_find=year_to_find).any():
                    year_indices.append(column_index-1)
                    for index, value in enumerate(column_b):
                        if not pd.isna(value) and 3 <= index <= 25:
                            variable_name = re.sub(r'\W+', '_', value)
                            values_dict_column_b.setdefault(variable_name, []).append(dataframe.iloc[index, column_index])
                    for index, value in enumerate(column_d):
                        if not pd.isna(value) and 31 <= index <= 45:
                            variable_name = re.sub(r'\W+', '_', value)
                            values_dict_column_d.setdefault(variable_name, []).append(dataframe.iloc[index, column_index])


            #Portfolio = (dataframe.iloc[[row_number], year_indices]).values.flatten()                 
            Portfolio = (df.iloc[[row_number], year_indices]).fillna(method='ffill', axis=1).values.flatten() #fills an empty value with the last value

            result_dataframe = pd.DataFrame.from_dict(values_dict_column_b, orient='index').T
            result_dataframe['Portfolio'] = Portfolio

            lines_of_businesses = len(result_dataframe)

            result_dataframe_column_d = pd.DataFrame.from_dict(values_dict_column_d, orient='index').T


            desired_order = ['1000', '500', '250', '200', '100', '50', '25', '10', '5', 'Exposure', 'Modelled_Exposure', 'Average_Annual_Loss']
            column_d_reordered = {key: values_dict_column_d[key] for key in desired_order}

            repeat_count = len(column_d_reordered)

            repeated_data = [result_dataframe.loc[[index]].reindex([index] * repeat_count) for index in result_dataframe.index]
            repeated_dataframe = pd.concat(repeated_data, ignore_index=True)

            Return_Period = list(column_d_reordered.keys()) * len(result_dataframe)
            Value = np.array(list(column_d_reordered.values())).T.ravel()

            if len(Return_Period) != len(Value):
                raise ValueError("Error: Length of column Return Period is not equal to length of Value")
            if len(Return_Period) != len(column_d_reordered) * len(result_dataframe):
                raise ValueError("Error: Length of variables does not match the number of lines of businesses")
            if len(repeated_dataframe) != len(Value):
                raise ValueError("Error: Length of repeated data and return periods don't match")

            dataframe_final = repeated_dataframe.copy()
            dataframe_final['Return Period'] = Return_Period
            dataframe_final['Value'] = Value
            dataframe_final.to_csv('output.txt', sep='\t', index=False)


            dataframe_final_renamed = dataframe_final.rename(columns=column_mapping)
            dataframe_final_renamed2 = dataframe_final_renamed[columns_in_table]
            return dataframe_final_renamed2

            def process_data_50(dataframe, year_to_find):

                def contains_year(cell, year_to_find):
                    return isinstance(cell, datetime) and cell.year == year_to_find

                values_dict_column_b = {}
                values_dict_column_d = {}
                column_b = dataframe.iloc[:, 1].astype(str)
                column_d = dataframe.iloc[:, 3].astype(str)

                year_indices = []
                row_number=26

                for column_index, column in enumerate(dataframe.columns):
                    if dataframe[column].apply(contains_year, year_to_find=year_to_find).any():
                        year_indices.append(column_index-1)
                        for index, value in enumerate(column_b):
                            if not pd.isna(value) and 3 <= index <= 25:
                                variable_name = re.sub(r'\W+', '_', value)
                                values_dict_column_b.setdefault(variable_name, []).append(dataframe.iloc[index, column_index])
                        for index, value in enumerate(column_d):
                            if not pd.isna(value) and 31 <= index <= 47:
                                variable_name = re.sub(r'\W+', '_', value)
                                values_dict_column_d.setdefault(variable_name, []).append(dataframe.iloc[index, column_index])


                Portfolio = (df.iloc[[row_number], year_indices]).fillna(method='ffill', axis=1).values.flatten()
                result_dataframe = pd.DataFrame.from_dict(values_dict_column_b, orient='index').T
                result_dataframe['Portfolio'] = Portfolio

                lines_of_businesses = len(result_dataframe)

                result_dataframe_column_d = pd.DataFrame.from_dict(values_dict_column_d, orient='index').T

                desired_order = ['1000', '500', '250', '200', '100', '50', '25', '10', '5', 'Exposure', 'Modelled_Exposure', 'Average_Annual_Loss', 'Standard_Deviation', 'Coefficient_of_Variation']
                column_d_reordered = {key: values_dict_column_d[key] for key in desired_order}

                repeat_count = len(column_d_reordered)

                repeated_data = [result_dataframe.loc[[index]].reindex([index] * repeat_count) for index in result_dataframe.index]
                repeated_dataframe = pd.concat(repeated_data, ignore_index=True)

                Return_Period = list(column_d_reordered.keys()) * len(result_dataframe)
                Value = np.array(list(column_d_reordered.values())).T.ravel()

                if len(Return_Period) != len(Value):
                    raise ValueError("Error: Length of column Return Period is not equal to length of Value")
                if len(Return_Period) != len(column_d_reordered) * len(result_dataframe):
                    raise ValueError("Error: Length of variables does not match the number of lines of businesses")
                if len(repeated_dataframe) != len(Value):
                    raise ValueError("Error: Length of repeated data and return periods don't match")

                dataframe_final = repeated_dataframe.copy()
                dataframe_final['Return Period'] = Return_Period
                dataframe_final['Value'] = Value


                dataframe_final_renamed = dataframe_final.rename(columns=column_mapping)
                dataframe_final_renamed2 = dataframe_final_renamed[columns_in_table]
                return dataframe_final_renamed2



            def process_data_65(dataframe, year_to_find):
                processed_data1 = process_data_48(df, year_to_find)
                df2 = df.copy() 
                df2.iloc[35:46] = df.iloc[55:66] 
                processed_data2 = process_data_48(df2, year_to_find)
                processed_data2['Measure'] = 'Net Pre CAT'

                concatenated_data = pd.concat([processed_data1, processed_data2], ignore_index=True)
                return  concatenated_data



            if __name__ == "__main__":
                #source = r"C:\Users\rajoshi\Desktop\Modelling_Results\2024\Gallagher Re - ERGO 2024 renewal modelling - results DE v2.xlsx"
                #destination = r"C:\Users\rajoshi\Desktop\Modelling_Results\2024\test.xlsx"
                #year = 2023

                source = source_entry.get()
                destination = destination_entry.get()
                year = int(year_entry.get()) 


                
                column_mapping = {
                    'Business_Unit_BU_': 'Business Unit',
                    'incl_Subperil': 'incl Subperil',
                    'Country_modelled_': 'Country modelled',
                    'Date_of_Portfolio': 'Date of Portfolio',
                    'Measure_Perspective': 'Perspective',
                    'Exchange_Rate': 'Exchange Rate',
                    'Data_Supplier': 'Data Supplier',
                    'NatCat_Model': 'NatCat Model',
                    'Model_Version': 'Model Version',
                    'Post_loss_amplification': 'Post Loss Amplification',
                    'Original_adjusted': 'original/adjusted'}


                
                columns_in_table= ['Business Unit', 'Peril', 'incl Subperil', 'Portfolio',  'original/adjusted', 'Modelling_ID', 'Country modelled', 'Date of Portfolio',
                                    'Perspective', 'Measure', 'Return Period', 'Value', 'Currency', 'Exchange Rate',
                                  'Data Supplier', 'Modeler', 'NatCat Model', 'Model Version', 'Post Loss Amplification', 'Comments']

                wb1 = xw.Book(source)

                # Define the destination path and ensure the destination file is removed if it exists
                if os.path.exists(destination):
                    os.remove(destination)

                # Create a new destination workbook
                wb2 = xw.Book()

                # Create a single destination sheet for all processed data
                destination_sheet = wb2.sheets.add()
                destination_sheet.name = "Processed Data"  # You can name it as needed

                # List of sheet names to copy from source to destination
                sheets_to_copy = [sheet.name for sheet in wb1.sheets if (sheet.visible and (sheet.name.rstrip().endswith("AEP") or sheet.name.rstrip().endswith("OEP")))]

            # Iterate through the sheets and append processed data to the destination sheet
                year = 2023
                c=0
                for sheet_name in sheets_to_copy:
                    display_output("Processing " + sheet_name + "....")
                    search_word = "AAL"
                    df = pd.read_excel(source, sheet_name)  # Read sheet data into a DataFrame
                    row_number = df[df.apply(lambda row: row.astype(str).str.contains(search_word).any(), axis=1)].index.max()+1
                    column_d_length = len(df.iloc[:, 3].astype(str))


                    try:
                        if row_number == 48:
                            processed_data = process_data_48(df, year_to_find=year)
                            display_output("Done....")
                            c += 1
                        elif row_number == 50:
                            processed_data = process_data_50(df, year_to_find=year)
                            display_output("Done....")
                            c += 1
                        elif row_number == 68:
                            processed_data = process_data_65(df, year_to_find=year)
                            display_output("Done....")
                            c += 1
                        else:
                    # Handle the case where none of the conditions are met
                             display_output(f"No processing option found for sheet {sheet_name}")
                    except Exception as e:
                         display_output(f"Error processing sheet {sheet_name}. Sheets does not adhere to length standards: {str(e)}")


                    if destination_sheet.range('A1').value is None:
                        # If the first cell is empty, start writing data from A1
                            destination_sheet.range('A1').options(index=False).value = processed_data
                    else:
                            last_row = len(destination_sheet.range('A1').expand('table').value)
                            processed_data.columns = [" "] * len(processed_data.columns)
                            destination_sheet.range((last_row + 1, 1)).options(index=False).value = processed_data.rename_axis(None, axis=1)

                if (c==len(sheets_to_copy)):
                        display_output("All sheets processed :)")


                # Copy a sheet with a name starting with "Cover" to the second position (1-based index) in the target workbook
                for sheet in wb1.sheets:
                    if re.match(r'^Cover', sheet.name):
                        ws1 = sheet
                        break

                if ws1 is not None:
                    ws1.api.Copy(Before=wb2.sheets(1).api)

                # Copy a sheet with a name starting with "Disclaim" to the last position in the target workbook
                for sheet in wb1.sheets:
                    if re.match(r'^Disclaim', sheet.name):
                        sheet.api.Copy(Before=wb2.sheets[-1].api)     

                # Save the destination workbook
                wb2.save(destination)
                wb2.app.quit()

    processing_thread = threading.Thread(target=process_data_thread)
    processing_thread.start()


            
            

def browse_source_file():
        file_path = filedialog.askopenfilename(filetypes=[("Excel Files", "*.xlsx")])
        source_entry.delete(0, tk.END)
        source_entry.insert(0, file_path)

def browse_destination_file():
        file_path = filedialog.asksaveasfilename(filetypes=[("Excel Files", "*.xlsx")])
        destination_entry.delete(0, tk.END)
        destination_entry.insert(0, file_path)

def display_output(text):
    output_text.insert(tk.END, text + "\n")
    output_text.see(tk.END)

# The main GUI window
window = tk.Tk()
window.title("ERGO Database Format Converter")
window.geometry("600x400")
window.eval('tk::PlaceWindow . center')

frame = tk.Frame(window)
frame.pack(expand=True)
source_label = ttk.Label(frame, text="Source Excel File:")
source_label.pack(pady=12)
source_entry = ttk.Entry(frame)
source_entry.pack(pady=5)
browse_source_button = ttk.Button(frame, text="Browse", command=browse_source_file)
browse_source_button.pack(pady=5)

destination_label = ttk.Label(frame, text="Destination Excel File in ERGO Format:")
destination_label.pack(pady=10)
destination_entry = ttk.Entry(frame)
destination_entry.pack(pady=5)
browse_destination_button = ttk.Button(frame, text="Browse", command=browse_destination_file)
browse_destination_button.pack(pady=5)
year_label = ttk.Label(frame, text="Year:")
year_label.pack(pady=10)
year_entry = ttk.Entry(frame)
year_entry.pack(pady=5)
process_button = ttk.Button(frame, text="Process Data", command=process_data)
process_button.pack(pady=10)

# Create a text widget for displaying the output
output_text = tk.Text(frame, wrap=tk.WORD, width=40, height=10)
output_text.pack(pady=10)
output_text.config(state=tk.DISABLED)  # Disable text editing in the output box

window.mainloop()