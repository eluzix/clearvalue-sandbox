import json

from clearvalue.lib import utils


def generate_ids():
    with open('/Users/uzix/Downloads/export.json', 'r') as f:
        js = json.load(f)

    for item in js:
        item['_id'] = utils.generate_id()

    with open('/Users/uzix/Downloads/export.json', 'w') as f:
        json.dump(js, f)


if __name__ == '__main__':
    generate_ids()
