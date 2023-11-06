import base64
import os
from google.cloud import pubsub_v1
from google.cloud import storage
from ast import literal_eval
import hashlib


def hello_pubsub(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    #get data from the 'bucket_to_mapper_topic' pub/sub
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    mapped = literal_eval(pubsub_message)

    #turns 2d array of [alphabatically ordered key of a word,  actual word] into a dict with (alpha, [words])
    #merges words with same alpha into same pair (puts anagrams together) and removes repeats. 
    # (alphas here refers to the alphabatically ordered key of words). Grouping anagrams together starts here.
    anagrams = dict()
    for pair in mapped:
        k = pair[0]
        v = pair[1]
        if k not in anagrams.keys():
            anagrams[k] = [v]
        else:
            values = anagrams[k]
            if v not in values:
                values.append(v)
                anagrams[k] = values

    #create a version of the anagrams dictionary where the keys are
    #hashed values of the original keys
    hashed_ana = dict()
    for key in anagrams:
        k = key
        v = anagrams[key]
        hashed_k = int(hashlib.sha256(k.encode('utf-8')).hexdigest(), base=16)
        hashed_ana[hashed_k] = v

    print(hashed_ana)
        
    #accesses the intermediate file called 'anagram_dictionaries.txt' in the 'anagram_dictionary_bucket'
    # bucket, all of the dictionaries created by this function will be stored here
    storage_client = storage.Client()
    bucket = storage_client.bucket('anagram_dictionary_bucket')
    blob = bucket.blob('anagram_dictionaries.txt')

    #overwrite the 'anagram_dictionaries.txt' file with the new data.
    #Counter used to add a tag of the book number at the end
    #of each dictionary, this ensures only the completed file (with all
    #3 dictionaries) is used by the next function that reads the file created.
    #-by only taking the data when the '3' tag is present at the very end of the file.
    count = 1
    with blob.open("r") as f:
        text = f.read()
    
    if text != "":
        count = int(text[-1]) + 1

    text = text + "\n" + str(hashed_ana) + str(count)
    with blob.open("w") as f:
        f.write(str(text))

    print("program finished writing to file")

####requirements
#google-cloud-pubsub>=0.28.1
#google-cloud-storage>=2.7.0
