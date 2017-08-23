# Ghost Game

from random import randint
print ('Ghost Game')

feeling_brave = True

score = 0
#now you can choice amount of doors ;-)
door_in_game = 10

while feeling_brave and door_in_game > 1:
    ghost_door = randint(1, door_in_game)
    print (door_in_game, ' doors ahead...')
    print ('A ghost behind one.')
    print ('Which door do you open?')
    door = input('Enter number of door  ')
    door_num = int(door)
    if door_num == ghost_door:
        print('GHOST!')
        feeling_brave = False
    else:
        print('No ghost!')
        print('You enter the next room.')
        score = score + 1
        if door_in_game > 2:
            door_in_game -= 1

print('Run away!')
print('Game over! You scored', score)