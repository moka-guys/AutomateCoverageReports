'''
Created on 16 Jan 2017
This script loops through a folder and calls the insert to db script for each coverage report before archiving the chanjo report into the imported sub-folder
@author: ajones7
'''

import os

directory="P:\Bioinformatics\NGS\depthofcoverage\genesummaries"
#directory="H:\\test"
for i in os.listdir(directory):
    if i.startswith("imported"):
        pass
    else:
        os.system("python F:\Moka\Files\Software\depthofcoverage\AutomateCoverageReports\insert_to_db.py -d " + str(i.split("_")[2]))
        os.rename(directory+"\\"+i, directory+"\imported\\"+i)
