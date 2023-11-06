import os
from google.cloud import pubsub_v1
from google.cloud import storage
from ast import literal_eval

def hello_gcs(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    file = event
    print(f"Processing file: {file['name']}.")

    #reads the intermediate file created by the shuffler function cslled 'anagram_dictionaries.txt'
    #with dictionaries of all the anagrams per book
    storage_client = storage.client.Client()
    bucket = storage_client.get_bucket('anagram_dictionary_bucket')
    blob = bucket.blob('anagram_dictionaries.txt')
    json_data = blob.download_as_string()
    data = json_data.decode('utf-8')

    #checks to make sure shuffler has finished adding all of the
    #dictionaries into the file by checking the tag of the last dictionary
    #-which matches which dictionary it is, so once the last one is loaded in
    #this function passess the data on.
    if data != "" and data[-1] == "3":
        #splist data back up into 3 dictionaries to create an array of dictionaries
        dicts = [None] * 3
        d = ""
        i = 0
        char = 0
        for char in range (len(data)):
            if data[char] != "}":
                d += data[char]
            else:
                d += "}"
                dicts[i] = (d)
                d = "" 
                i += 1

        j = 0
        for j in range (len(dicts)):
            if j == 0:
                new_d = dicts[j][1:len(dicts[j])]
                dicts[j] = literal_eval(new_d)
            else:
                new_d = dicts[j][2:len(dicts[j])]
                dicts[j] = literal_eval(new_d)

        #initialise the 10 dictionaries that will be sent to the reducer
        dict0 = dict()
        dict1 = dict()
        dict2 = dict()
        dict3 = dict()
        dict4 = dict()
        dict5 = dict()
        dict6 = dict()
        dict7 = dict()
        dict8 = dict()
        dict9 = dict()

        #based on the hashed key of each (alpha, [word]) pair in each dictionary, split the pairs up between the 
        #10 dictionaries initialised above, but making usre that all of those with the same 'alpha' 
        # (same alphabetical ordering of word) and so the same hashed key, are sent to the same dictionary. This ensures
        #that all words that are anagrams of each other go to the same dictionary and so to the same reducer since each dict is sent
        #as a separate message to the pub/sub between this function and the reducer. Allows the same reducer to put all of the words thata are 
        # anagrams of each other together. 
        for dictionary in dicts:
            for key in dictionary:
                if ((key % 10) == 0):
                    if key in dict0.keys(): 
                        dict0[key].append(dictionary[key])
                    else:
                        dict0[key] = [dictionary[key]]
                elif ((key % 10) == 1):
                    if key in dict1.keys(): 
                        dict1[key].append(dictionary[key])
                    else:
                        dict1[key] = [dictionary[key]]
                elif ((key % 10) == 2):
                    if key in dict2.keys(): 
                        dict2[key].append(dictionary[key])
                    else:
                        dict2[key] = [dictionary[key]]
                elif ((key % 10) == 3):
                    if key in dict3.keys(): 
                        dict3[key].append(dictionary[key])
                    else:
                        dict3[key] = [dictionary[key]]
                elif ((key % 10) == 4):
                    if key in dict4.keys(): 
                        dict4[key].append(dictionary[key])
                    else:
                        dict4[key] = [dictionary[key]]
                elif ((key % 10) == 5):
                    if key in dict5.keys(): 
                        dict5[key].append(dictionary[key])
                    else:
                        dict5[key] = [dictionary[key]]
                elif ((key % 10) == 6):
                    if key in dict6.keys(): 
                        dict6[key].append(dictionary[key])
                    else:
                        dict6[key] = [dictionary[key]]
                elif ((key % 10) == 7):
                    if key in dict7.keys(): 
                        dict7[key].append(dictionary[key])
                    else:
                        dict7[key] = [dictionary[key]]
                elif ((key % 10) == 8):
                    if key in dict8.keys(): 
                        dict8[key].append(dictionary[key])
                    else:
                        dict8[key] = [dictionary[key]]
                elif ((key % 10) == 9):
                    if key in dict9.keys(): 
                        dict9[key].append(dictionary[key])
                    else:
                        dict9[key] = [dictionary[key]]

        print("0:   " + str(dict0))
        print("1:   " + str(dict1))
        print("2:   " + str(dict2))
        print("3:   " + str(dict3))
        print("4:   " + str(dict4))
        print("5:   " + str(dict5))
        print("6:   " + str(dict6))
        print("7:   " + str(dict7))
        print("8:   " + str(dict8))
        print("9:   " + str(dict9))

        #publishes each dictionary from dict0 ... dict9 as a separate message to the 'shuffler_to_reducer_topic' pub/sub 
        # so that it goes to a separate reducer 
        publisher = pubsub_v1.PublisherClient()
        topic_path = 'projects/refined-circuit-366910/topics/shuffler_to_reducer_topic'

        future = publisher.publish(topic_path, str(dict0).encode('utf-8'))
        future = publisher.publish(topic_path, str(dict1).encode('utf-8'))
        future = publisher.publish(topic_path, str(dict2).encode('utf-8'))
        future = publisher.publish(topic_path, str(dict3).encode('utf-8'))
        future = publisher.publish(topic_path, str(dict4).encode('utf-8'))
        future = publisher.publish(topic_path, str(dict5).encode('utf-8'))
        future = publisher.publish(topic_path, str(dict6).encode('utf-8'))
        future = publisher.publish(topic_path, str(dict7).encode('utf-8'))
        future = publisher.publish(topic_path, str(dict8).encode('utf-8'))
        future = publisher.publish(topic_path, str(dict9).encode('utf-8'))

####requirements
#google-cloud-pubsub>=0.28.1
#google-cloud-storage>=2.7.0
