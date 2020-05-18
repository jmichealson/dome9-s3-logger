#!/usr/bin/env/python
# requires python3+ (e.g. python --version)

import json
import sys
import os

def other(some_name):
    print(f"Hi, {some_name['name']}! This is the other function!")
    
def main(argv=None):
    print('This is the main function at runtime!')
    name_dict = {'name': 'Patrick'}
    #Let's call another function!
    other(name_dict)

if __name__ == "__main__":
    sys.exit(main())