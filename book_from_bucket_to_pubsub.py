import os
from google.cloud import pubsub_v1
from google.cloud import storage

def get_books(request):
    
    request_json = request.get_json()
    
    #accesses the bucket with the input ebook files
    storage_client = storage.client.Client()
    bucket = storage_client.get_bucket('100_ebook_bucket')

    #gets the data as a string of 3 of the ebooks
    blob1 = bucket.get_blob("8956.txt")
    json_data1 = blob1.download_as_string()
    blob2 = bucket.get_blob("9043.txt")
    json_data2 = blob2.download_as_string()
    blob3 = bucket.get_blob("9047.txt")
    json_data3 = blob3.download_as_string()

    #specifies topic to which the function will be publishing the books to
    publisher = pubsub_v1.PublisherClient()
    topic_path = 'projects/refined-circuit-366910/topics/bucket_to_mapper_topic'

    #since this is the first function, it will erase all of the previous data
    #stored in the intermediate 'anagram_dictionaries.txt' file and the final output 
    #file 'anagrams.txt'
    storage_client = storage.Client()
    bucket = storage_client.bucket('anagram_dictionary_bucket')
    blob = bucket.blob('anagram_dictionaries.txt')

    with blob.open("w") as f:
        f.write("")

    storage_client = storage.Client()
    bucket = storage_client.bucket('100_ebook_anagrams')
    blob = bucket.blob('anagrams.txt')

    with blob.open("w") as f:
        f.write("")

    #sends each ebook as a separate message to the pub/sub topic 'bucket_to_mapper_topic'
    future = publisher.publish(topic_path, json_data1)
    future = publisher.publish(topic_path, json_data2)
    future = publisher.publish(topic_path, json_data3)

    #returns a message on the cloud shell terminal indicating that the function has completed running
    return("Program initiated\n")

####requirements file
#google-cloud-pubsub>=0.28.1
#google-cloud-storage>=2.7.0