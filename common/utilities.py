import configparser
import io
import os
import os.path
import pickle
import re
from itertools import permutations
from os import path
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from numpy.linalg import norm


class Utilities:
    def __init__(self):
        pass

    def get_latest_file(self, directory, startswith):
        path = Path(directory)
        files = list()
        for i in path.iterdir():
            if startswith in str(i):
                files.append(str(i))
        return sorted(files)[-1]

    def replace_newline(self, text):
        return text.replace('\n', '').strip()

    def calc_cosine(self, a, b):
        return (a @ b.T) / (norm(a) * norm(b))

    def strip_html(self, text):
        """
        Replace html with class-specific string.
        :param text: raw text
        :return: clean text
        """
        regexr = re.compile(r'<[^>]*(>|$)|&nbsp;|&zwnj;|&raquo;|&laquo;|&gt;|&amp;/g')
        return re.sub(regexr, '', text)

    def replace_url(self, text):
        """
        Replace urls of the format http://www.domain.com or https://www.google.com with class-specific string.

        :param text: raw text
        :return: clean text
        """
        regexr = re.compile(r'\w+:\/{2}[\d\w-]+(\.[\d\w-]+)*(?:(?:\/[^\s/]*))*')
        return re.sub(regexr, '', text)

    def replace_phone(self, text):
        """
        Replace phone numbers by with class-specific string.

        They have to be in the following format ddd-ddd-dddd(111.222.4444)
        or ddd.ddd.dddd(111-222-3333).

        :param text: raw text
        :return: clean text
        """
        regexr = re.compile(r'\d{10}')
        text = re.sub(regexr, 'PHONE', text)
        regexr = re.compile(r'(\d{3}[-\.\s]\d{3}[-\.\s]\d{4}|\(\d{3}\)'
                            r'\s*\d{3}[-\.\s]\d{4}|\d{3}[-\.\s]\d{4})')
        return re.sub(regexr, 'PHONE', text)

    def replace_email(self, text):
        """
        Replace emails with class-specific string.

        :param text: raw text
        :return: clean text
        """
        regexr = re.compile(r'[\w\.-]+@[\w\.-]+')
        text = re.sub(regexr, 'EMAIL', text)
        return text

    def property_loader(self, property_file):
        config = None
        if path.exists(property_file):
            try:
                config = configparser.RawConfigParser()
                config.read(property_file)
            except Exception as e:
                raise ValueError(e)
        return config

    def write_dict_to_file(self, dictionary, file, delimiter=';', rev=True):
        writer = open(file, 'w')
        for k, v in dictionary.items():
            if rev:
                writer.write(v + delimiter + k)
            else:
                writer.write(k + delimiter + v)
            writer.write('\n')
        return None

    def write_list_to_file(self, arraylist, file):
        writer = open(file, 'w')
        for item in arraylist:
            writer.write(item)
            writer.write('\n')
        writer.close()
        return None

    def read_stopwords(self, file):
        stopwords = open(file, 'r').readlines()
        stopwords = [w.replace('\n', '') for w in stopwords]
        stopwords = [i.strip().lower() for i in stopwords]
        return stopwords

    def remove_stopwords(self, text, stopwords):
        return ' '.join(list(filter(lambda x: (x not in stopwords), text.split(' '))))

    def remove_terminal_stopwords(self, text, stopwords):
        text = text.strip()
        tokens = text.split(' ')

        # strip stopwords from left
        n = 0
        while n < len(tokens) and tokens[n] in stopwords:
            n = n + 1
        # created new token array with {left striped stopwords}
        tokens = tokens[n:]

        # strip stopwords from right
        n = len(tokens) - 1
        while n > 0 and tokens[n] in stopwords:
            n = n - 1
        # created new token array with {right striped stopwords}
        tokens = tokens[0:n + 1]

        return ' '.join(tokens).strip()

    def remove_punct(self, text):
        text = re.sub(r'[^\w\s]', '', text)
        text = re.sub('\s+', ' ', text).strip()
        return text

    def tokenize_sentence(self, sent, remove_punctuation=False):
        sent = sent.replace(',', ' ').replace(';', ' ').replace('|', ' ')
        sent = re.sub('\s+', ' ', sent).strip()
        if remove_punctuation:
            sent = self.remove_punct(sent)
        tokens = list()
        for token in sent.split(' '):
            if len(token) > 0:
                if token[-1] == '.':
                    if token.index('.') == len(token) - 1:
                        tokens.append(token[:-1])
                    else:
                        tokens.append(token)
                else:
                    tokens.append(token)
        return tokens

    def sentence_detector(self, para, remove_punctuation=False):
        delim = '%delim%'
        para = para.replace(delim, ' ')
        para = re.sub('\s+', ' ', para).strip()
        if remove_punctuation:
            para = self.remove_punct(para)
        tokens = list()
        for token in para.split(' '):
            if len(token) > 0:
                if token[-1] == '.':
                    if token.index('.') == len(token) - 1:
                        tokens.append(token[:-1])
                        tokens.append(delim)
                    else:
                        tokens.append(token)
                else:
                    tokens.append(token)
        sentences = ' '.join(tokens).split(delim)
        sentences = [i.strip() for i in sentences if len(i.strip()) > 0]
        return sentences

    def get_all_permutation(self, text):
        all_list = list()

        parts = text.split(' ')
        if len(parts) == 1 or len(parts) > 4:
            return [text]

        string_comb = ''
        for i in range(len(parts)):
            string_comb = string_comb + str(i)
        if len(parts) == 2:
            perms = [list(permutations(string_comb, 2))]
        if len(parts) == 3:
            perms = [list(permutations(string_comb, 2)), list(permutations(string_comb, 3))]
        if len(parts) == 4:
            perms = [list(permutations(string_comb, 2)), list(permutations(string_comb, 3)),
                     list(permutations(string_comb, 4))]

        for perm in perms:
            for indices in perm:
                li = list()
                for i in indices:
                    li.append(parts[int(i)])
                res = ' '.join(li)
                all_list.append(res)
        return all_list

    def generate_ngrams(self, arr, which_gram=[1, 2, 3, 4]):
        result = list()
        for n in which_gram:
            grams = [' '.join(arr[i:i + n]) for i in range(len(arr) - n + 1)]
            grams = [i for i in grams if i != '']
            result += grams
        return result

    def generate_ngrams_indices(self, arr, word_index, which_gram=[1, 2, 3, 4]):
        result = list()
        for n in which_gram:
            grams = [(' '.join(arr[i:i + n]), i, word_index[i]) for i in range(len(arr) - n + 1)]
            grams = [i for i in grams if i != '']
            result += grams
        return result

    def load_pickle(self, pickle_file_path):
        object = None
        if os.path.exists(pickle_file_path):
            with open(pickle_file_path, "rb") as fp:
                object = pickle.load(fp)
        return object

    def load_model_from_bytes_format(self, model_path):
        """Save all gensim models in bytes format and load them in bytes format.

        Args:
            model_path ([type]): Modle path.

        Returns:
            [type]: model
        """

        with open(model_path, 'rb') as ofh:
            model_data = pickle.load(ofh)
        return model_data

    def save_model_to_byte_format(self, model, save_path):

        if not isinstance(model, io.BytesIO):
            object_content = io.BytesIO()
            pickle.dump(model, object_content)
            object_bytes = object_content.getvalue()
            with open(save_path, 'wb') as ofh:
                ofh.write(object_bytes)
            return True
        return False

    def read_pkl(self, file):
        data = None
        with open(file, 'rb') as f:
            data = pickle.load(f)
        return data

    def save_pkl(self, obj, file):
        ack = -1
        with open(file, 'wb') as handle:
            pickle.dump(obj, handle, protocol=pickle.HIGHEST_PROTOCOL)
            ack = 1
        return ack

    def clean_text(self, text):
        text = text.replace('\n', '').replace('( )+', ' ').strip()
        text = ' '.join(text.split(" "))
        return text

    def clean_text_d2v(self, text: str) -> str:
        """Apply text cleansing in sequence."""
        # Replace Urls
        text = self.replace_url(text)

        # Strip HTML tags
        text = self.strip_html(text)

        # Replace phone numbers
        text = self.replace_phone(text)

        # Replace emails
        text = self.replace_email(text)

        # Remove unwanted brackets
        text = re.sub(r'[\(\)\{\}\[\]-_]', " ", text)

        # Remove unwanted chars
        text = re.sub(r'[^A-Za-z0-9_.+ ]', "", text)

        # Replace any numbers
        text = re.sub("[0-9]+", "NUM", text)

        # Remove any multiple spaces
        text = re.sub(r'\s+', " ", text)

        return text.encode("ascii", errors="ignore").decode().strip()

    def read_url(self, url):
        try:
            url_content = requests.get(url)
        except:
            return None
        return url_content

    def read_bs4_url(self, url_content):
        soup = None
        try:
            soup = BeautifulSoup(url_content.text)
        except:
            return None
        return soup

    def read_bs4_html(self, html):
        soup = None
        try:
            soup = BeautifulSoup(html)
        except:
            return None
        return soup

