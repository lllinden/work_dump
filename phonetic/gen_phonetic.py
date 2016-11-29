#-*-coding:utf-8-*-
import sys
from gen_mods1 import *
if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("usage: python gen_phonetic.py input_file out_file")
        exit(-1)
    context_file_str = sys.argv[1]
    phonetic_file_str = sys.argv[2]

    context_file_object = open(context_file_str, "r+")
    words_list = context_file_object.readlines()
    # print words_list
    context_file_object.close()
    words_list_lower = []
    with open(context_file_str, 'r') as fileinput:
        for line in fileinput:
            lines = line.lower()
            if len(lines)>2:
                if "." not in line:
                    words_list_lower.append(lines)
            else:
                pass

    phonetic_list = map(get_kk_json, words_list_lower)
    with open(phonetic_file_str, 'w+') as f:
        for phonetic in phonetic_list:
            f.write(phonetic+"\n")
