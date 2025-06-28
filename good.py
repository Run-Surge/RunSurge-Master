import pandas as pd

def main():
#     # Example usage of pandas
    data = {'Name': ['Alice', 'Bob', 'Charlie'],
            'Age': [25, 30, 35]}
    df = pd.DataFrame(data)
    sum_df = df['Age'].sum()
    print(f"Sum of ages: {sum_df}")
main()