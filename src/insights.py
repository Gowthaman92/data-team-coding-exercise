import pandas as pd
import sys


def get_json_df(file_path):

    try:
        df = pd.read_json(file_path,lines=True)
        return df
    #Missing file. Exception need to be specifically handled
    except Exception as e:
        print(e)
        return None

def get_csv_df(file_path):

    try:
        df = pd.read_csv(file_path)
        return df
    
    #Missing file. Exception need to be specifically handled
    except Exception as e:
        print(e)
        return None
  

def transform_orders(df):
    df['units'] = df.units.apply(lambda x: x.items())
    df = df.explode('units')
    df[['componentId', 'count']] = pd.DataFrame(df['units'].tolist(), index=df.index)
    return df[['componentId', 'count']]


def main():
    arguments = sys.argv

    if len(arguments) < 4:
        print('Missing arguments!')
        return -1
    elif len(arguments) > 4:
        print('More arguments supplied!')
        return -1

    path = arguments[1]
    components_file = arguments[2]
    orders_file = arguments[3]

    components_df = get_csv_df(path + '/' + components_file)
    orders_json_df = get_json_df(path + '/' + orders_file)

    if components_df is None or orders_json_df is None:
        print('Error with file')
        return -1

    orders_json_df = orders_json_df[orders_json_df['timestamp'].dt.date.astype(str) == '2021-06-03']
    orders_df = transform_orders(orders_json_df)

    merged_df = pd.merge(orders_df, components_df, on='componentId')

    result = merged_df.groupby('colour').agg({'count' : sum})
    print(result)


if __name__ == "__main__":
    main()