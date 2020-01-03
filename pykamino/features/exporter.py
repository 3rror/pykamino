from os import path
import csv


def features_to_csv(feature_set, pathname, basename):
    for product, feats in feature_set.items():
        feats = iter(feats)
        first_row = next(feats)
        with open(f'{path.join(pathname, basename)}_{product}.csv', 'w') as csv_file:
            writer = csv.DictWriter(csv_file, first_row.keys())
            writer.writeheader()
            writer.writerow(first_row)
            writer.writerows(feats)
