from multiprocessing.dummy import Pool as ThreadPool
import boto3
import datetime
import sys


# Some constants
BUCKET_NAME = ''
REGION = ''
OLD_FOLDER = ''
NEW_FOLDER = ''
CLIENT = boto3.client('s3', REGION)


def build_prefix(base: str, subpref: list):
    """Builds a prefix based on a base string and a list of sub prefixes"""
    # Have to use double quote inside of string formatting
    return f'{base}/{"/".join(str(x) for x in subpref)}'


def get_object_count(prefix):
    """Returns the number of objects under a prefix"""
    all_contents = []
    marker =  None 
    kwargs = {'Bucket': BUCKET_NAME, 'Prefix': prefix}
    while True:
        res = CLIENT.list_objects_v2(**kwargs)
        try:
            all_contents.extend(res['Contents'])
        except KeyError:
            # Prefix doesnt exist!
            print('Folder doesnt exist')
            return 0
    
        if res['IsTruncated']:
            kwargs['ContinuationToken']= res['NextContinuationToken']
        else:
            break
    return len(all_contents)


def get_objects(prefix):
    """Returns the contents of the objects under a prefix"""
    all_contents = []
    kwargs = {'Bucket': BUCKET_NAME, 'Prefix': prefix}
    while True:
        res = CLIENT.list_objects_v2(**kwargs)
        all_contents.extend(res['Contents'])
        if res['IsTruncated']:
            kwargs['ContinuationToken']= res['NextContinuationToken']
        else:
            break
    return all_contents


def should_copy(old, new):
    """Returns if two prefixes have the same amount of data"""
    return get_object_count(old) != get_object_count(new)


def exists(prefix):
    """Checks if a file exists at a specific prefix"""
    kwargs = {'Bucket': BUCKET_NAME, 'Prefix': prefix}
    res = CLIENT.list_objects_v2(**kwargs)
    try:
        res['Contents']
    except KeyError:
        # Prefix doesnt exist!
        return False
    return True


def copy(data):
    """Copies files from one bucket to another"""
    old = data['old']
    new = data['new']

    # If the file does not exist in the destination, copy it
    if not exists(new):
        try:
            # In BUCKET_NAME copy from CopySource to Key
            CLIENT.copy_object(Bucket=BUCKET_NAME, CopySource=f'{BUCKET_NAME}/{old}', Key=new)
            return True
        except:
            print(f'Failed to copy {old} to {new}')
            return False


def process(lst: list, function, processes: int):
    """Handles multiprocessing using ThreadPool; sends items from a list to a function and gets the results as a list"""
    # Define the number of processes, use less than or equal to the defined value
    count_threads = min(processes, len(lst))
    if count_threads == 0:
            return []
    pool = ThreadPool(count_threads)

    # Tell the user what is happening
    print(f"Copying {len(lst)} items using {function} in {count_threads} processes.")

    # Calls function() and returns True for success and False for fail each call to a lst
    result = (pool.imap_unordered(function, lst))
    pool.close()

    # Display progress as the scraper runs its processes
    while (len(lst) > 1):
        completed = result._index

        # Break out of the loop if all tasks are done or if there is only one task
        if (completed == len(lst)):
            sys.stdout.flush()
            sys.stdout.write('\r' + "")
            sys.stdout.flush()
            break

        # Avoid a ZeroDivisionError
        if completed > 0:
            sys.stdout.flush()
            sys.stdout.write('\r' + f"{completed/len(lst)*100:.0f}% done. {len(lst)-completed} left. ")
            sys.stdout.flush()
        sys.stdout.flush()

    pool.join()
    return list(result)


def handle(self, *args, **options):
    # get_object_count(client, "stream/30423")
    maps = get_objects(f'{OLD_FOLDER}/')
    # May need to do some additional pruning here

    # This is the path of the existing folder and the new folder
    prefix = build_prefix(OLD_FOLDER, [aws_id, aws_idx])
    new_prefix = build_prefix(NEW_FOLDER, [id_])

    if should_copy(prefix, new_prefix):
        # Get the contents of the folder we need to move
        print(f'{prefix} moves to {new_prefix}')
        contents = [obj['Key'] for obj in get_objects(prefix)]

        """
        Multiprocess copying can only take one arg
        Generate a dict of {old: 'path', new: 'path2'}
        """
        copies = []
        for f in contents:
            copies.append({'old': f, 'new': f'{new_prefix}{f.replace(prefix, "")}'})

        a = datetime.datetime.now()
        # Process the array of dicts
        res = process(copies, copy, 32)
        
        # If any items failed to upload, let us know
        if any(item == False for item in res):
            print(f'{len([x for x in res if x == False])} failed copy operations.')

        # Print the time the copy operations took
        b = datetime.datetime.now()
        print(f'{len(contents)} copy operations in {(b - a).seconds // 60}:{(b - a).seconds % 60}')

    else:
        # If we already copied the folder who cares?
        print(f'Skipping!')
