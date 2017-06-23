#!/usr/bin/env python
# encoding: utf-8
"""
A module to recursively find and remove duplicate files.

A SHA1 hash will be used to calculate identical files.
All state will be pickled to disk so that progress can be resumed.
This program will be intended to run multi-threaded.
"""
import argparse
import logging as logging_lib
import os
import pickle

flags = None
logging = None

LOG_FORMAT = '%(levelname)s %(asctime)-15s %(filename)s:%(lineno)s] %(message)s'
DATE_FORMAT = '%m%d %H:%M:%S'
DEFAULT_CACHE_DIR = '.rdups_cache'
QUEUE_CACHE_FILE = 'queue'


def setup():
  """Runs required setup before a search.

  This will recursively list all directories to be used as a queue and pickled
  to file.
  """
  queue_filepath = os.path.join(flags.cache_dir, QUEUE_CACHE_FILE)
  if os.path.exists(queue_filepath):
    with open(queue_filepath, 'r') as queue_file:
      unpickler = pickle.Unpickler(queue_file)
      # Get the dict indexed by root dirs.
      queue = unpickler.load()
  else:
    queue = {}

  if flags.search_dir in queue:
    logging.info('Search already setup, %d items remaining' %
        len(queue[flags.search_dir]))
  else:
    logging.info('Starting setup...')
    queue[flags.search_dir] = list_dirs(flags.search_dir)
    logging.info('Pickling dirs..., queue size: %d' %
        len(queue[flags.search_dir]))
    logging.debug(queue)
    with open(queue_filepath, 'w') as queue_file:
      pickler = pickle.Pickler(queue_file)
      pickler.dump(queue)


def list_dirs(path):
  """Recursively lists all dirs under path."""
  children = os.listdir(path)
  if not children and flags.prune_empty_dirs:
    # Remove the empty dir.
    remove_empty_dir(path)
    # Don't add this dir to the list since it is empty.
    return []

  # Recurse in and check for children.
  dirs = []
  for subdir in children:
    subdir_path = os.path.join(path, subdir)
    if os.path.isdir(subdir_path):
      dirs.extend(list_dirs(subdir_path))

  # List dirs again, it could be empty now!
  if len(os.listdir(path)):
    dirs.append(path)
  else:
    remove_empty_dir(path)
    # Don't add this dir to the list since it is empty.
    return []

  return dirs


def remove_empty_dir(path):
  """Removes an empty directory."""
  logging.info('Removing empty dir: %s' % path)
  os.rmdir(path)


def init_cache():
  """Initializes the flags.cache_dir if not yet initialized."""
  if not os.path.exists(flags.cache_dir):
    os.mkdir(flags.cache_dir)
  global logging
  logging.info('cache_dir: %s' % flags.cache_dir)


def parse_args():
  # Set up command line argument parsing.
  parser = argparse.ArgumentParser(
      formatter_class=argparse.RawTextHelpFormatter,
      description='Recursively find and remove duplicate files.')

  # Parse the action.
  parser.add_argument('action', type=str, choices=['setup', 'search'],
      help='The action to perform.')

  # Global config.
  parser.add_argument('--cache_dir', type=str,
      default=os.path.join(os.path.expanduser('~'), DEFAULT_CACHE_DIR),
      help='Directory to store program state.')
  parser.add_argument('--search_dir', required=True,
      help='Directory to be recursively searched for duplicates.')

  # Other options.
  parser.add_argument('--prune_empty_dirs', action='store_true', default=False,
      help='Delete\'s empty directories while running setup, for speed.')

  # Parse command line arguments
  return parser.parse_args()


def main():
  global flags
  global logging

  # Set up logging.
  logging_lib.basicConfig(format=LOG_FORMAT, datefmt=DATE_FORMAT,
      level=logging_lib.INFO)
  logging = logging_lib.getLogger('remove_dupes')

  flags = parse_args()

  init_cache()
  if flags.action == 'setup':
    setup()
  else:
    logging.error('Bad command')


if __name__ == '__main__':
  main()
