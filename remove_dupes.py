#!/usr/bin/env python
# encoding: utf-8
"""
A module to recursively find and remove duplicate files.

A SHA1 hash will be used to calculate identical files.
All state will be pickled to disk so that progress can be resumed.
"""
import argparse
import hashlib
import logging as logging_lib
import os, os.path
import pickle
import re
import signal
import uuid

flags = None
logging = None

LOG_FORMAT = '%(levelname)s %(asctime)-15s %(filename)s:%(lineno)s] %(message)s'
DATE_FORMAT = '%m%d %H:%M:%S'
DEFAULT_CACHE_DIR = '.rdups_cache'
QUEUE_CACHE_FILE = 'queue'
HASH_CACHE_FILE = 'hash'
DEFAULT_TRASH_DIR = '.rdups_trashed'
# Signals a shutdown.
quit = False

def get_queue_file_path():
  return os.path.join(flags.cache_dir, QUEUE_CACHE_FILE)

  
def get_queues_from_cache():
  queue_filepath = get_queue_file_path()
  if os.path.exists(queue_filepath):
    with open(queue_filepath, 'r') as queue_file:
      unpickler = pickle.Unpickler(queue_file)
      # Get the dict indexed by root dirs.
      queue = unpickler.load()
  else:
    queue = {}
    
  return queue


def write_queues_to_cache(queues):
  queue_filepath = get_queue_file_path()
  with open(queue_filepath, 'w') as queue_file:
    pickler = pickle.Pickler(queue_file)
    pickler.dump(queues)
 

def get_hash_file_path():
  return os.path.join(flags.cache_dir, HASH_CACHE_FILE)

  
def get_hashes_from_cache():
  hash_filepath = get_hash_file_path()
  if os.path.exists(hash_filepath):
    with open(hash_filepath, 'r') as hash_file:
      unpickler = pickle.Unpickler(hash_file)
      # Get the dict indexed by root dirs.
      hashes = unpickler.load()
  else:
    hashes = {}
    
  return hashes


def write_hashes_to_cache(hashes):
  hash_filepath = get_hash_file_path()
  with open(hash_filepath, 'w') as hash_file:
    pickler = pickle.Pickler(hash_file)
    pickler.dump(hashes)

 
def setup():
  """Runs required setup before a search.

  This will recursively list all directories to be used as a queue and pickled
  to file.
  """
  queues = get_queues_from_cache()

  if flags.search_dir in queues:
    logging.info('Search already setup, %d items remaining' %
        len(queues[flags.search_dir]))
  else:
    logging.info('Starting setup...')
    queues[flags.search_dir] = list_dirs(flags.search_dir)
    logging.info('Pickling dirs..., queue size: %d' %
        len(queues[flags.search_dir]))
    logging.debug(queues)
    write_queues_to_cache(queues)


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

  
def search():
  """Processes the queue and calculates a hash for each file in the folder."""
  queues = get_queues_from_cache()

  if flags.search_dir not in queues:
    logging.error('Search path not set up: %r' % flags.search_dir)
    return 
    
  queue = queues[flags.search_dir]    
  logging.info('Search setup, %d items remaining' % len(queue))
  
  signal.signal(signal.SIGINT, set_quit)
  
  hashes = get_hashes_from_cache()
  hashes.setdefault(flags.search_dir, {})

  while queue and not quit:
    hashes[flags.search_dir] = process_one_folder(
        queue.pop(0), hashes[flags.search_dir])
    write_hashes_to_cache(hashes)
    queues[flags.search_dir] = queue
    write_queues_to_cache(queues)


def set_quit(_unused, _):
  """Registers a request to stop execution at the next iteration."""
  global quit
  quit = True
  logging.info('Quit requested!')

  
def process_one_folder(path, hashes):
  logging.info('Processing %r...' % path)
  children = os.listdir(path)
  for f in children:
    f_path = os.path.join(path, f)
    if not os.path.isdir(f_path):
      sha1 = process_one_file(f_path)
      hashes.setdefault(sha1, [])
      hashes[sha1].append(f_path)
  
  return hashes
  
def process_one_file(path):
  BUF_SIZE = 2**20  # lets read stuff in 64kb chunks!
  sha1 = hashlib.sha1()

  with open(path, 'rb') as f:
    while True:
      data = f.read(BUF_SIZE)
      if not data:
          break
      sha1.update(data)
 
  return sha1.hexdigest()


def status():
  """Gets the current state of all searches, started or completed."""
  queues = get_queues_from_cache()

  logging.info('Setup run on: %r' % queues.keys())
  
  if flags.search_dir not in queues:
    logging.error('Search path not set up: %r' % flags.search_dir)
    return 
    
  queue = queues[flags.search_dir]    
  logging.info('Search setup, %d items remaining' % len(queue))
  
  hashes = get_hashes_from_cache()
  logging.info('Hashes found: %r', hashes.get(flags.search_dir))


def analyze():
  """List the restults of a given search."""
  queues = get_queues_from_cache()

  logging.info('Setup run on: %r' % queues.keys())
  
  if flags.search_dir not in queues:
    logging.error('Search path not set up: %r' % flags.search_dir)
    return 
    
  queue = queues[flags.search_dir]    
  logging.info('Search setup, %d items remaining' % len(queue))
  
  hashes = get_hashes_from_cache()
  if flags.search_dir in hashes:
    logging.info('Hashes found: %r', len(hashes.get(flags.search_dir)))
  
    dups = 0
    for k, v in hashes[flags.search_dir].iteritems():
      if len(v) > 1:
        logging.info('Dups Found: %r' % v)
        dups += 1
        
    logging.info('Total Dups: %r' % dups)


def cleanup():
  """Delete's duplicates found by a search."""
  queues = get_queues_from_cache()

  if flags.search_dir not in queues:
    logging.error('Search path not set up: %r' % flags.search_dir)
    return 
    
  queue = queues[flags.search_dir]
  logging.info('Search setup, %d items remaining' % len(queue))
  
  hashes = get_hashes_from_cache()
  if flags.search_dir not in hashes:
    logging.info('Duplicates not found!')
    return
  
  signal.signal(signal.SIGINT, set_quit)
  trash_dir = os.path.abspath(os.path.join(flags.search_dir, os.pardir,
                                           flags.trash_dir))
  if not os.path.exists(trash_dir): 
    os.makedirs(trash_dir)
  for k, v in hashes[flags.search_dir].iteritems():
    if len(v) > 1:
      logging.info('Dups Found: %r' % v)
      kept = cleanup_one_entry(v, trash_dir)
      if kept:
        hashes[flags.search_dir][k] = kept
        write_hashes_to_cache(hashes)
    
    if quit:
      break
  

def cleanup_one_entry(dups, trash_dir):
  """Handles one set of duplicates."""
  regexp = re.compile('\(\d+\)')
  remove = filter(regexp.search, dups)

  for d in remove:
    logging.info('Moving %r to trash...' % d)
    if os.path.exists(d):
      try:
        os.rename(d, os.path.join(trash_dir, os.path.basename(d)))
      except OSError:
        os.rename(d, os.path.join(trash_dir, str(uuid.uuid4())[:4] + '-' + os.path.basename(d)))
 
  return list(set(dups) - set(remove))
 
 
def reset():
  """Deletes all existing data for a directory."""
  queues = get_queues_from_cache()

  if flags.search_dir in queues:
    queues.pop(flags.search_dir)
    write_queues_to_cache(queues)

  hashes = get_hashes_from_cache()
  if flags.search_dir in hashes:
    hashes.pop(flags.search_dir)
    write_hashes_to_cache()


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
  parser.add_argument('action', type=str,
      choices=['analyze', 'setup', 'search', 'status', 'cleanup', 'reset'],
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
  parser.add_argument('--trash_dir', type=str, default=DEFAULT_TRASH_DIR,
      help='Where to move the duplicates that are to be deleted. A relative'
           'path from the parent of the search dir.')
      
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
  elif flags.action == 'search':
    search()
  elif flags.action == 'status':
    status()
  elif flags.action == 'analyze':
    analyze()
  elif flags.action == 'cleanup':
    cleanup()
  elif flags.action == 'reset':
    reset()
  else:
    logging.error('Bad command')


if __name__ == '__main__':
  main()
