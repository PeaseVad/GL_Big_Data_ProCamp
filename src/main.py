import pandas as pd
from pathlib import Path


def calculate_metrics():
    downloads_path = str(Path.home() / "Downloads")
    print(downloads_path)
    df = pd.read_csv(downloads_path + r"\test-task_dataset_summer_products.csv", sep=',')
    df_result = df.groupby('origin_country').agg(
        {'price': ['mean'], 'rating_five_count': ['sum'], 'rating_count': ['sum']}).sort_values(by=['origin_country'])
    df_result['five_percentage'] = (df_result['rating_five_count'] / df_result['rating_count']) * 100
    df_result.fillna(0, inplace=True)
    df_result.drop(columns=['rating_five_count', 'rating_count'], axis=1, inplace=True)
    print(df_result)


if __name__ == '__main__':
    calculate_metrics()
