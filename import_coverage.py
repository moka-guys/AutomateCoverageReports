'''
Created on 16 Jan 2017

@author: ajones7
'''

import os

directory="P:\Bioinformatics\NGS\depthofcoverage\genesummaries"
#directory="H:\\test"
for i in os.listdir(directory):
    if i.startswith("imported"):
        pass
    else:
        #print "python F:\Moka\Files\Software\depthofcoverage\AutomateCoverageReports\import_depth_of_coverage.py -d "+str(i.split("_")[2])
        #print i
        os.system("python F:\Moka\Files\Software\depthofcoverage\AutomateCoverageReports\import_depth_of_coverage.py -d " + str(i.split("_")[2]))
        os.rename(directory+"\\"+i, directory+"\imported\\"+i)
