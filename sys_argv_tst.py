from sys import argv,exit
usr_input = argv[1]
usr_action = argv[2]
if(len(argv) != 3):
    print(f'Please follow the input format:\n{argv[0]} <your_input> <lower|upper|title>')
    exit()
if(usr_action == 'lower'):
    print(usr_input.lower())
elif(usr_action == 'upper'):
    print(usr_input.upper())
elif(usr_action == 'title'):
    print(usr_input.title())
else:
    print('Wrong input')


