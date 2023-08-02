import sys
  
current_word = None
current_count = 0
word = None


for line in sys.stdin:
    
    try:
        line = line.strip()
        word, count = line.split(',')

        count = int(count)
    
        if current_word == word:
            current_count +=count
        
        else:
            if current_word:
                print(current_word, current_count)
            current_count = count
            current_word = word
    except:
        continue


if current_word == word:
    print(current_word, current_count)

