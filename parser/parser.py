import sys
import os


def create_directory(dir_path):
    if not os.path.exists(dir_path):
        os.mkdir(dir_path)
        print "Directory " + dir_path + " Created "
    else:
        print "Directory " + dir_path + " already exists "


create_directory(sys.argv[1])
dir_path = sys.argv[1] + "/stock"
create_directory(dir_path)
file_path = sys.argv[2]

with open(file_path) as inputFile:
    inputFile.readline()
    line = inputFile.readline()
    symbol = ''
    outputFile = None
    while line:
        parts = line.split('|')
        if parts[0] != 'END':
            current_Symbol = parts[2]
            if symbol != current_Symbol:
                symbol = current_Symbol
                current_Symbol = current_Symbol.replace(" ", "_")
                print 'Symbol: ' + symbol

                if outputFile is not None and not outputFile.closed:
                    outputFile.close()

                create_directory(dir_path + "/" + current_Symbol)
                outputFile = open(dir_path + "/" + current_Symbol + "/" + current_Symbol + "_" + file_path[-8:] +
                                  ".txt", "w")

            outputFile.writelines(line)
        line = inputFile.readline()

    outputFile.close()
