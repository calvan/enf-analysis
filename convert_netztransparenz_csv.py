import argparse
from pathlib import Path

import pandas as pd


def convert_csv_netztransparenz(csv_file):
    csv_file_path = Path(csv_file)
    recorded_enf = pd.read_csv(csv_file,
                               usecols=[0, 1, 4], parse_dates=[['Datum', 'von']], skiprows=0, sep=';', decimal=',')

    recorded_enf.rename(columns={'Datum_von': 'time'}, inplace=True)
    data = recorded_enf.set_index('time').squeeze()
    new_filename = f'{str(csv_file_path.parent)}/{str(csv_file_path.stem)}_.csv'
    print(f'writing converted csv file as {new_filename}')
    data.to_csv(new_filename)


if __name__ == "__main__":
    argparser = argparse.ArgumentParser()
    argparser.add_argument("csv_file", help="csv file from netztransparenz.de")
    args = argparser.parse_args()

    convert_csv_netztransparenz(args.csv_file)