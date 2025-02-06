'''
def togglechar(str):
    anslist=[]
    for ch in str:
        if ch>='A' and ch<='Z':
            anschar = chr(ord(ch)+32)
            anslist.append(anschar)
        else:
            anschar = chr(ord(ch) - 32)
            anslist.append(anschar)
    ansstr = ''.join(anslist)
    return ansstr
str = 'EtlQALabs'
print('The input is ',str)
print('The output is ',togglechar(str))
'''
'''
#meaning of the code
def togglechar(str):
    anslist = []  # Initialize an empty list to store the toggled characters
    for ch in str:  # Iterate through each character in the input string
        if 'A' <= ch <= 'Z':  # Check if the character is an uppercase letter
            anschar = chr(ord(ch) + 32)  # Convert uppercase to lowercase
            anslist.append(anschar)  # Append the toggled character to the list
        else:  # If the character is a lowercase letter
            anschar = chr(ord(ch) - 32)  # Convert lowercase to uppercase
            anslist.append(anschar)  # Append the toggled character to the list
    ansstr = ''.join(anslist)  # Convert the list back to a string
    return ansstr  # Return the final toggled string

'''
def togglechar(str):
    return str.swapcase()  # Swaps uppercase letters to lowercase and vice versa

# Example input
str = 'EtlQALabs'
print('The input is:', str)
print('The output is:', togglechar(str))