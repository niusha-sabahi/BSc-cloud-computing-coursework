import base64
import os
from google.cloud import pubsub_v1

def hello_pubsub(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """

    #list of stop words to not include 
    stop_words = ["'tis", "'twas", 'a', 'able', 'about', 'across', 'after', "ain't", 'all', 'almost',
                  'also', 'am', 'among', 'an', 'and', 'any', 'are', "aren't", 'as', 'at', 'be', 'because',
                  'been', 'but', 'by', 'can', "can't", 'cannot', 'could', "could've", "couldn't", 'dear',
                  'did', "didn't", 'do', 'does', "doesn't", "don't", 'either', 'else', 'ever', 'every',
                  'for', 'from', 'get', 'got', 'had', 'has', "hasn't", 'have', 'he', "he'd", "he'll",
                  "he's", 'her', 'hers', 'him', 'his', 'how', "how'd", "how'll", "how's", 'however', 'i',
                  "i'd", "i'll", "i'm", "i've", 'if', 'in', 'into', 'is', "isn't", 'it', "it's", 'its',
                  'just', 'least', 'let', 'like', 'likely', 'may', 'me', 'might', "might've", "mightn't",
                  'most', 'must', "must've", "mustn't", 'my', 'neither', 'no', 'nor', 'not', 'of', 'off',
                  'often', 'on', 'only', 'or', 'other', 'our', 'own', 'rather', 'said', 'say', 'says', "shan't",
                  'she', "she'd", "she'll", "she's", 'should', "should've", "shouldn't", 'since', 'so',
                  'some', 'than', 'that', "that'll", "that's", 'the', 'their', 'them', 'then', 'there',
                  "there's", 'these', 'they', "they'd", "they'll", "they're", "they've", 'this', 'tis',
                  'to', 'too', 'twas', 'us', 'wants', 'was', "wasn't", 'we', "we'd", "we'll", "we're",
                  'were', "weren't", 'what', "what'd", "what's", 'when', 'when', "when'd", "when'll",
                  "when's", 'where', "where'd", "where'll", "where's", 'which', 'while', 'who', "who'd",
                  "who'll", "who's", 'whom', 'why', "why'd", "why'll", "why's", 'will', 'with', "won't", 'would',
                  "would've", "wouldn't", 'yet', 'you', "you'd", "you'll", "you're", "you've", 'your']

    #retrieve the sbooks from the "bucket_to_mapper_topic" pub/sub    
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    #split them up into individual words
    msg = pubsub_message.split()

    #add all of the words from an ebook into 'new_msg' list if it's not a stop word and does not contain any
    #non-alphabet characters
    new_msg = []
    for j in range (len(msg)):
        in_stop_words = stop_words.count(msg[j].lower())
        if in_stop_words == 0 and msg[j].isalpha():
            new_msg.append(msg[j])

    #create another list, the same length as 'new_msg' called 'alpha_msg' to store all of the alphabetical ordered 
    #versions of each word in 'new_msg' in the same order. (e.g. 'elt' for 'let')
    alpha_msg = []
    for w in new_msg:
        w = w.lower()
        alpha_msg.append(''.join(sorted(w)))

    #create a new 2D array called 'mapped' that pairs all of the words with their alphabetical ordered 
    #versions.
    mapped = []
    i = 0
    for i in range (len(new_msg)):
        mapped.append([alpha_msg[i], new_msg[i].lower()])

    #publish the 'mapped' array to the 'mapper_to_shuffler_topic' pub/sub
    publisher = pubsub_v1.PublisherClient()
    topic_path = 'projects/refined-circuit-366910/topics/mapper_to_shuffler_topic'

    data = str(mapped).encode('utf-8')
    future = publisher.publish(topic_path, data)

    print(str(mapped))

####requirements
#google-cloud-pubsub>=0.28.1