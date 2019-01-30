import csv


def features_to_csv(features, path):
    with open(path, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, features[0].keys())
        writer.writeheader()
        writer.writerows(features)
