# BSc-cloud-computing-coursework
This repository contains my coursework for my Cloud Computing module, completed during my BSc Computer Science Degree. 

The task was to create a program that, given some ebooks, can create a list of all anagrams of words which appear in all of the ebooks combined. Each anagram must consist of at least 2 words and be alphabetically ordered.

This project was created on Google Cloud Platform (GCP), and so can only be run there. GCP Buckets, Cloud Functions and several Pub/Sub topics were utlised to create this program. A MapReduce architecture was used for this project and operated as follows:

1. ebooks are stored in the input bucket.
2. 'book_from_bucket_to_pubsub' function sends books from the input bucket to the 'bucket_to_mapper_topic' pub/sub.
3. 'mapper_function' gets data from the pub/sub and maps ebooks into (key, value) pairs and sends them to the 'mapper_to_shuffler_topic' pub/sub.
4. 'shuffler-function' gets data from the pub/sub and creates dictionaries for each ebook, grouping anagrams in the same book together, then stores them all in 'anagram_dictionaries.txt' file.
5. 'shuffler_to_reducer_function' gets data from the 'anagram_dictionaries.txt' file and sorts and splits data up between reducer instances, then sends them over to the 'shuffler_to_reducer_topic' pub/sub.
6. 'reducer_function' gets data from the pub/sub, removes repeated words, merges all anagrams, puts anagrams into lists and writes output to the 'anagrams.txt' file in the output bucket, where the list of anagrams from all of the ebooks combined can be viewed.

The files in this repository are all of the Cloud Function files written in Python for this project. Please read the comments in the code for further description of how the program works.

## Intended Usage and Restrictions
This repository is intended for code visibility and reference purposes only. Users are allowed to:
- **View** the source code on GitHub.
- **Clone** the repository for personal reference.

However, please note the following restrictions:
- You are **not permitted** to modify, distribute, or use the code in this repository for any other purposes, including commercial use, without explicit permission from the author.
- Attribution to the original author is appreciated but not required.

It is crucial to respect these restrictions to comply with the author's intentions and rights. If you have any questions or need further clarification regarding usage, please contact the author.

## Contact
For any inquiries or permissions regarding the usage of this code, please contact:
Niusha Sabahi (mailto:sabahi.niusha@gmail.com)
