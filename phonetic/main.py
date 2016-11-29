#coding:utf-8
# -*- coding:utf-8 -*-  
# Converts English Text into IPA format.

import gen_mods

if __name__ == '__main__':
    print('English Text to IPA Converter\n')
    userInput = input('[English]: ').split()
    print 'here', userInput
    IPA_CMU = gen_mods.getIPA_CMU(userInput)
    print(gen_mods.get_final(IPA_CMU))

