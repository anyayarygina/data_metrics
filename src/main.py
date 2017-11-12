import argparse
import logging
import yaml
import sys

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='process one hour of data')
    parser.add_argument('--config', type=str, required=True,
                        help='relative path to yml config, for example ./config/default.yml')
    args = parser.parse_args()

    logging.basicConfig(format=u'%(levelname)-8s [%(asctime)s] %(message)s',
                        level=logging.INFO,
                        filename='./log/framework.log')

    sys.path.append('./src/framework')
    sys.path.append('./src/modules')

    from framework import Framework

    with open(args.config, 'r') as outfile:
        config = yaml.safe_load(outfile)

    framework = Framework(config)

    framework.process()
