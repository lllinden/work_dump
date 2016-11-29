hive -e "select distinct text from model.oral_question" > question.txt
sed 's/\([A-Z]\)/\L\1/g' question.txt  > question_lower.txt
sed 's/\(\.\|\,\|\!\|?\|\.\.\.\|\…\)//g' question_lower.txt > question_lower_1.txt
sed '/[[:digit:]]/d' question_lower_1.txt > question_lower_2.txt
sed -e 's/^[ \t]*//' question_lower_2.txt > question_lower_3.txt
sed -e 's/^[\t ]*//' question_lower_3.txt > question_lower_4.txt
sed '/[a-z]\{2\}/d' question_lower_4.txt > question_lower_5.txt
