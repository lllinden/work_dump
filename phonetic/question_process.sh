# hive -e "select distinct text from model.oral_question" > question.txt
#convert upper case to lower case
sed '/[[:digit:]]/d' question.txt > 1.txt 
sed 's/\([A-Z]\)/\L\1/g' 1.txt > 2.txt
sed 's/\.\|\,\|\?\|\!\|\:\|\…\|=//g' 2.txt > 1.txt
sed '/^\(\w\)\1$/d' 1.txt > 2.txt
sed '/^\(\w\)\s\1$/d' 2.txt>1.txt
sed 's/^ *\| *$//g' 1.txt > 2.txt
sed 's/  / /g' 2.txt > 1.txt
sed '/ [a-z]$/d' 1.txt > 2.txt
sed '/^[b-z] /d' 2.txt>1.txt
sed '/ \(\w\) /d' 1.txt > 2.txt 
sed 's/  / /g' 2.txt > 1.txt
sed '/bow/d' 1.txt > question_processed.txt

sort question_processed.txt | uniq -u > question_u.txt
# nohup python gen_phonetic.py question_u.txt question_pohonetic.txt &







