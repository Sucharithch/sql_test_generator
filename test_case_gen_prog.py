import os
import pandas as pd
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

def generate_test_cases(df):
    prompt = prompt_test_cases(df)
    client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
    MODEL_NAME = "gpt-4o"
    try:
        response = client.chat.completions.create(
            
            messages=[
                {"role": "user", "content": prompt}
            ],
            model=MODEL_NAME
        )
        response = response.choices[0].message.content
        # print(response)
        # rows = response.strip().split('\n')
        # columns = ['target_table','target_column','target_datatype','source_table','source_column','transformation_logic', 'test_case_description']
        # df = pd.DataFrame(columns=columns)
        # for row in rows:
        #     split_row = [col.strip() for col in row.split(' | ')]
        #     if len(split_row) == 7:
        #         df = df.append(pd.Series(split_row, index=columns), ignore_index=True)
        return response

    except Exception as e:
        return f"Error generating test cases: {str(e)}"

def prompt_test_cases(row):
    return f"""
Task: You are an ETL tester and SQL expert. A row will be provided with the following column structure: ['target_table', 'target_column', 'target_datatype', 'source_table', 'source_column', 'transformation_logic'].
Instructions:
provied row:
{row}
write test cases for row  and return output. Also write the query required to test that
Query for test case should be written in such a way that no records are retrieved if the transformation logic is applied properly on the source column. Need not print 'PASS' or 'FAIL'.
Once that is written, assign unique test case name for that test case
Do not include anything like Explanation, Note etc, Only give raw outputstring with '|' as delimiter for each columns data
in the query do not use place holders like source columns and source tables just use information from {row}
in the query use information about source table and source columns from ```row[source_table]``` and ```row[source_column]``` i.e in 4th and 5th columns counting from 1.
in the query use information about target table and target columns from ```row[target_table]``` and ```row[target_column]``` i.e in 1st and 2nd columns counting from 1.
in output give like target_table|target_column|target_datatype|source_table|source_column|transformation_logic|test case name|test case|sql query
"""

def test_case_generation(input_directory):
    base_dir = os.path.dirname(input_directory)
    output_directory = os.path.join(base_dir, "test_case_generated_files")
    os.makedirs(output_directory, exist_ok=True)
    for file_name in os.listdir(input_directory):
        file_path = os.path.join(input_directory, file_name)
        if file_name.endswith('.csv'):
            df = pd.read_csv(file_path)
            columns = [
                'target_table', 'target_column', 'target_datatype', 'source_table',
                'source_column', 'transformation_logic', 'test_case_name',
                'test_case_description', 'test_case_query'
            ]
            new_df = pd.DataFrame(columns=columns)
            for _, row in df.iterrows():
                result = generate_test_cases(row)
                print(result)  
                rows = result.strip().split('\n')  
                for row_string in rows:
                    row_data = row_string.split('|') 
                    row_data = [field.strip() for field in row_data]  
                    if len(row_data) == len(columns):  
                        new_row = pd.DataFrame([row_data], columns=columns)
                        new_df = pd.concat([new_df, new_row], ignore_index=True)
            output_file_path = os.path.join(output_directory, f"{file_name.split('.')[0]}_12_test_cases.csv")
            new_df.to_csv(output_file_path, index=False)

    return new_df

if __name__ == "__main__":
    input_directory = r'C:\Users\sathwika.akella\test_case_generation\sqltcgen\archive_rq_files'
    test_case_generation(input_directory)
