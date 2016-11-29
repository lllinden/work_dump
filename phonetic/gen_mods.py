#-*-coding:utf-8-*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import re
words = []
element = []

file_object = open("CMU_phonetic_dictionary.txt", 'r+')  
file_lines = file_object.readlines()
for line in file_lines:
    words.append(" "+line.lower())

def getIPA_CMU(userInputRaw):
    userInputRaw = userInputRaw.strip('"')
    userInput =[]
    if ' ' in userInputRaw:
         userInput= userInputRaw.split(' ')
    else:
        userInput.append(userInputRaw)
    IPA_CMUT = ""; ele_num = 0; mult_num = []
    for w in userInput:
        if w == "a":
            IPA_CMUT += "a "
        w = re.sub("[^a-zA-Z]", "", w)
        w = " " + w.strip().lower() + " "
        w2 = " " + w.strip().lower() + "("
        for string in words:
            if w in string:
                ele_num = ele_num + 1
                element.append(ele_num)
                IPA_CMU = string.replace(w, "").strip().replace(" ", "")
                IPA_CMU = re.sub("[0-9]", "", IPA_CMU)
                IPA_CMUT += IPA_CMU + " "
            if w2 in string:
                multiple = []; mult_num = []
                mult_num.append(ele_num)
                multiple.append(string)
                get_multiple(multiple,w2)
                multiple = get_IPA(get_multiple(multiple,w2))
            if w == string:
                pass      
    # aslist = IPA_CMUT.split()
    # if mult_num:
    #     print 'multiple entries found: '
    #     for m in mult_num:
    #         try:
    #             old = aslist[m-1]
    #             print aslist[m-1]
    #         except IndexError:
    #             print userInput
    #         aslist[m-1] = aslist[m-1].replace(old, multiple) 
    #         print aslist[m-1]
    #         asliststring = " ".join(aslist)
            # get_finalPrint(asliststring)  
    # print IPA_CMUT   
    return IPA_CMUT; 


def get_multiple(multiple,w2):
    IPA_CMU = ""
    IPA_CMUT = ""
    for CMU_strings in multiple: 
        IPA_CMU = CMU_strings.replace(w2, "").strip().replace(" ", "")
        IPA_CMU = re.sub("[0-9]", "", IPA_CMU)
        IPA_CMU = re.sub('[()]', "", IPA_CMU)
        IPA_CMUT += IPA_CMU
    return IPA_CMUT; 



def get_IPA(input):
    IPA_CMU = getIPA_CMU(input)
    #convert CMU to standard IPA
    IPA = "" # final string
    IPA_CMU = IPA_CMU.split()
    for w in IPA_CMU:
        # print IPA_CMU
        if w == "a":   w = w.replace("a", "ə,") ### ago
        if "ey" in w:  w = w.replace("ey", "e,") ### say
        if "aa" in w:  w = w.replace("aa", "ɑ,") ### hot
        if "ae" in w:  w = w.replace("ae", "æ,") ### fat
        if "ah" in w:  w = w.replace("ah", "ə,") ### ago
        if "ao" in w:  w = w.replace("ao", "ɔ,") ### call
        if "aw" in w:  w = w.replace("aw", "aʊ,") ### house
        if "ay" in w:  w = w.replace("ay", "aɪ,") ### high
        if "ch" in w:  w = w.replace("ch", "ʧ,") ### church
        if "dh" in w:  w = w.replace("dh", "ð,") ### there
        if "eh" in w:  w = w.replace("eh", "ɛ,") ### head
        if "er" in w:  w = w.replace("er", "ər,") ### sister
        if "hh" in w:  w = w.replace("hh", "h,") ### who
        if "ih" in w:  w = w.replace("ih", "ɪ,") ### exit
        if "jh" in w:  w = w.replace("jh", "ʤ,") ### vegetable
        if "ng" in w:  w = w.replace("ng", "ŋ,") ### sing
        if "ow" in w:  w = w.replace("ow", "oʊ,") ### blow
        if "oy" in w:  w = w.replace("oy", "ɔɪ,") ### boy
        if "sh" in w:  w = w.replace("sh", "ʃ,") ### social
        if "th" in w:  w = w.replace("th", "θ,") ### thank
        if "uh" in w:  w = w.replace("uh", "ʊ,") ### look
        if "uw" in w:  w = w.replace("uw", "u,") ### flew
        if "zh" in w:  w = w.replace("zh", "ʒ,") ### pleasure
        if "iy" in w:  w = w.replace("iy", "i,") ### sea
        if "y" in w:   w = w.replace("y", "j,") ### year
        
        if "b" in w:   w = w.replace("b", "b,")
        if "t" in w:   w = w.replace("t", "t,")
        if "c" in w:   w = w.replace("c", "c,")
        if "d" in w:   w = w.replace("d", "d,")
        if "f" in w:   w = w.replace("f", "f,")
        if "g" in w:   w = w.replace("g", "g,")
        ####if "h" in w:   w = w.replace("h", "h,")
        if "k" in w:   w = w.replace("k", "k,")
        if "l" in w:   w = w.replace("l", "l,")
        if "m" in w:   w = w.replace("m", "m,")
        if "n" in w:   w = w.replace("n", "n,")
        if "p" in w:   w = w.replace("p", "p,")
        if "q" in w:   w = w.replace("q", "q,")
        if "r" in w:   w = w.replace("r", "r,")
        if "s" in w:   w = w.replace("s", "s,")
        if "t" in w:   w = w.replace("t", "t,")
        if "v" in w:   w = w.replace("v", "v,")
        if "w" in w:   w = w.replace("w", "w,")
        if "x" in w:   w = w.replace("x", "x,")
        if "z" in w:   w = w.replace("z", "z,")
        IPA += w + ","
    return(IPA.strip());

# def get_final(IPA_CMU):
#     final_IPA = get_IPA(IPA_CMU)
#     final_IPA
#     return final_IPA;

# def get_finalRAW(IPA_CMU):
#     final_IPA_RAW = get_IPA(IPA_CMU)
#     return final_IPA_RAW;

# def get_finalPrint(IPA_CMU):
    # final_IPA = get_IPA(IPA_CMU)
def get_kk(input):
    return get_IPA(input)

