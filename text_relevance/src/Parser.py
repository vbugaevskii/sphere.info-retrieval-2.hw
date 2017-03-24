from bs4 import BeautifulSoup
from HTMLParser import HTMLParser, HTMLParseError

import os
import sys
import re
import codecs
import commands

from urlparse import urlparse

from Normalizer import Normalizer


normalizer = Normalizer()


def filter_text(phrase):
    phrase = phrase.lower()
    phrase = re.sub(r'[\W\d_]+', ' ', phrase, flags=re.UNICODE)
    phrase = re.sub(r'\s+', ' ', phrase, flags=re.UNICODE)
    return phrase.strip()


def normalize_text(normalizer, phrase):
    phrase = phrase.split()
    phrase = map(normalizer.get_normal_form, phrase)
    phrase = ' '.join(phrase)
    return phrase


class TextDOCParser:
    def __init__(self):
        pass
        
    @staticmethod
    def parse(file_name):        
        doc = commands.getoutput("catdoc -w {}".format(file_name))
        doc = doc.decode('utf-8')
        doc = normalize_text(normalizer, filter_text(doc))
        return doc


class TextHTMLParser(HTMLParser):
    tag_current = None

    def __init__(self):
        HTMLParser.__init__(self)
        self.doc_desc = TextHTMLParser.__get_new_dict_instance()
        
    @staticmethod
    def __get_new_dict_instance():
        return {
            "title": "",
            "description": "",
            "keywords": "",
            "text": ""
        }

    def handle_starttag(self, tag, attrs):
        attrs_as_dict = dict(attrs)

        if tag == 'meta':
            name = attrs_as_dict.get('name', '')
            if name in ['keywords', 'description']:
                self.doc_desc[name] = attrs_as_dict.get('content', '').strip()

        self.tag_current = tag

    def handle_data(self, data):
        data = data.strip()
        if data:
            if self.tag_current == 'title':
                self.doc_desc['title'] += ' ' + data
            elif self.tag_current not in ['script', 'style']:
                self.doc_desc['text'] += '\n' + data

    def get_document_parsed(self):
        for key, value in self.doc_desc.iteritems():
            value = filter_text(value)
            value = normalize_text(normalizer, value)
            self.doc_desc[key] = value
        return self.doc_desc
        
    def clear(self):
        self.doc_desc = TextHTMLParser.__get_new_dict_instance()


class Parser:
    def __init__(self):
        pass

    @staticmethod
    def get_charset(file_name):
        variants = []
        
        with open(file_name, 'r') as f_name:
            url = f_name.next().strip()
            url = urlparse(url).netloc
            
            koi_urls = ['subscribe.ru', 'kulichki.com']
            if any([koi_url in url for koi_url in koi_urls]):
                return ['koi8-R']
            
            if 'terton.ru' in url:
                return ['utf-8'] 
                
        with open(file_name, 'r') as f_name:    
            soup = BeautifulSoup(f_name, 'html.parser')
            desc = soup.findAll("meta")
            for meta in desc:
                metaname = meta.get('charset', '').lower()
                if metaname:
                    variants.append(metaname)
                    
                metaname = meta.get('content', '').lower()
                metaname = re.findall("charset\s*=\s*\S*", metaname)
                if metaname:
                    metaname = metaname[0]
                    metaname = re.sub(r'\s+', '', metaname)
                    metaname = metaname[len("charset="):]
                    if metaname:
                        variants.append(metaname)
        
        variants = map(lambda x: x.strip(), variants)
        return variants

    @staticmethod
    def parse_with_encoding(parser, file_name, encoding, errors='strict'):
        try:
            with codecs.open(file_name, mode='r', encoding=encoding, errors=errors) as f_doc:
                url = f_doc.next().strip()
                parser.feed(f_doc.read())
            return url
        except LookupError:
            return None
        except UnicodeError:
            return None
        except HTMLParseError:
            return None

    @staticmethod
    def is_doc_file(file_name):
        with open(file_name, 'r') as f_read:
            url = f_read.next().strip()
            url = urlparse(url).path
            _, ext = os.path.splitext(url)
        return "doc" in ext

    @staticmethod
    def parse(file_name):
        if Parser.is_doc_file(file_name):
            return Parser.parse_as_doc(file_name)
        else:
            return Parser.parse_as_html(file_name)

    @staticmethod
    def parse_as_doc(file_name):
        with open(file_name, 'r') as f_read:
            url = f_read.next().strip()
            
            with open("empty.doc", "w") as f_write:
                for line in f_read:
                    f_write.write(line)
        
        result = TextDOCParser.parse("empty.doc")
        
        os.remove("empty.doc")
        
        result = {
            "title": "",
            "description": "",
            "keywords": "",
            "text": result,
            "url": url,
            "encoding": "utf-8"
        }
        return result

    @staticmethod
    def parse_as_html(file_name):
        parser = TextHTMLParser()
        parser.clear()

        url = None

        try:
            encodings_found = Parser.get_charset(file_name)
        except HTMLParseError:
            encodings_found = []
        
        if encodings_found:
            encoding = encodings_found[0]
            url = Parser.parse_with_encoding(parser, file_name, encoding, errors='ignore')
        
        if url is None:
            for encoding in encodings_found + ['utf-8', 'windows-1251', 'koi8-R']: 
                url = Parser.parse_with_encoding(parser, file_name, encoding)
                if url:
                    break

        if url is None:
            return None

        result = parser.get_document_parsed()        
        result["url"] = url
        result["encoding"] = encoding
        return result
        

class ParsedDocumentFormat:
    def __init__(self):
        pass
    
    @staticmethod
    def dump(doc, file_name):
        field_order = ["source", "encoding", "index", "url", "title", "description", "keywords", "text"]
    
        if not all([field in doc for field in field_order]):
            raise NameError("Some fields are not found!")
    
        with codecs.open(file_name, mode="w", encoding="utf-8") as f_result:
            for name in field_order:
                f_result.write(u'{}\t{}\n'.format(name, doc[name]));

    @staticmethod
    def load(file_name):
        doc = dict()
        
        with codecs.open(file_name, mode="r", encoding="utf-8") as f_result:
            for items in f_result:
                items = items.strip().split('\t')
                
                if len(items) > 2:
                    items = [items[0], ' '.join(items[1:])]
                elif len(items) == 1:
                    items.append("")
                elif len(items) == 0:
                    continue
                    
                doc[items[0]] = items[1]
        
        doc["index"] = int(doc["index"])        
        return doc


if __name__ == '__main__':
    if len(sys.argv) == 2:
        file_name = sys.argv[1]
    else:
        # file_name = "../data/0/-1926367218506042044"
        # file_name = "../data/22/-2166406324772577473"
        # file_name = "../data/8/4264547756929936657"
        # file_name = "../data/0/-8179272985712893380"
        # file_name = "../data/0/4582681362703649294"
        # file_name = "../data/0/2763395349736301729"
        file_name = "../data/21/-8157717159353957369"
        
    result = Parser.parse(file_name)
    if result is None:
        exit(0)
        
    result["index"] = -1
    result["source"] = file_name

    for name, value in result.iteritems():
        print u'-' * 10
        print u'{}: {}'.format(name, value)

    ParsedDocumentFormat.dump(result, 'result')
    result_ = ParsedDocumentFormat.load('result')
    
    os.remove('result')
        
    if result != result_:
        for key in result.keys():
            print key, result[key] == result_[key]
        raise NameError("Results differ!")
