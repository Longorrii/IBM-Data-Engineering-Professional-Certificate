"""This module provides functions for arithmetic operations."""

def add(number1, number2):
    '''Add two numbers and return the result.'''
    return number1 + number2

NUM1 = 4
NUM2 = 5  # Changed variable name to uppercase as per convention
TOTAL = add(NUM1, NUM2)  # Changed variable name to uppercase as per convention
print(f"The sum of {NUM1} and {NUM2} is {TOTAL}")  # Changed to use f-string for string formatting
