with open('dog_breeds_reversed.txt', 'w') as writer:
    a = 4
    writer.write(f'{str(a)}\n')

with open('dog_breeds_reversed.txt', 'a') as writer:
    writer.write('b')