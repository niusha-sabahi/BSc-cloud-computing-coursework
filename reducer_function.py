import base64
from ast import literal_eval
from google.cloud import storage

def hello_pubsub(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    #retreive data from the 'shuffler_to_reducer_topic' pub/sub sent from the 'shuffler_to_reducer_function'
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    data = literal_eval(pubsub_message)

    #sorts all anagrams together and removed duplicates, also sorts the anagram list into alphabetical order
    anagrams = []
    for key in data:
        words = data[key]
        unique_words = []
        for i in words:
            for j in i:
                if j not in unique_words:
                    unique_words.append(j)
        if len(unique_words) > 1:
            anagrams.append(sorted(unique_words))

    print(anagrams)

    #creates a peice of text out of all lists of anagrams from the message this instance of the reducer received
    #and puts each anagram list on a separate line
    new_text = ""
    for l in anagrams:
        new_text += str(l) + "\n"

    #writes the text created into the output file 'anagrams.txt' in the '100_ebook_anagrams' bucket, adding to the 
    #other anagram lists that may have already been added by the other reducer instances.
    storage_client = storage.Client()
    bucket = storage_client.bucket('100_ebook_anagrams')
    blob = bucket.blob('anagrams.txt')

    with blob.open("r") as f:
        text = f.read()    

    text = text + "\n" + new_text
    with blob.open("w") as f:
        f.write(str(text))

#requirements
#google-cloud-storage>=2.7.0
