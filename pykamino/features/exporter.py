import csv


def features_to_csv(features, path):
    with open(path, 'w') as csv_file:
        writer = csv.DictWriter(csv_file, features[0].keys())
        writer.writeheader()
        writer.writerows(features)
